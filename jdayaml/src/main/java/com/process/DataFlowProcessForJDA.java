package com.process;

import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.utils.Constants;
import com.utils.SchemaDef;
import com.utils.SchemaDef.DelimiterType;
import com.utils.SchemaField;
import com.utils.SchemaUtils;

public class DataFlowProcessForJDA implements Constants {
	static Logger log = Logger.getLogger(DataFlowProcessForJDA.class.getName());
	
	public static void run(String Schema, String successTable, String errorTable, String templocation, 
			final String filepath,String archivePath,final String header, DelimiterType delimiter,final String fileName) throws Exception {
	
				try{
					log.info("PipeLine Flow Start");
					// TODO Auto-generated method stub
					final SchemaDef schemaDef = new SchemaDef(delimiter, Schema);
									
					DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
					options.setRunner(BlockingDataflowPipelineRunner.class);
					options.setProject(PROJECT_NAME);
					options.setTempLocation(templocation);
					options.setStagingLocation(templocation);
					
					
					//PipelineOptions options = PipelineOptionsFactory.create();
					
					// Then create the pipeline.
					Pipeline p = Pipeline.create(options);
					log.info("PipeLine Flow reading file from Storage Start");
					PCollection<String> lines = p.apply(TextIO.Read.named("ReadDataFromStorage").from(filepath)); //gs://sap_thd/Promo_Week.csv"));
					
					
					final TupleTag<TableRow> outputRows = new TupleTag<TableRow>() {
						private static final long serialVersionUID = -6477121291763113902L;
					};
					final TupleTag<String> invalideItems = new TupleTag<String>() {
						private static final long serialVersionUID = 5739658952429469825L;
					};
					final TupleTag<TableRow> invalideItemLogs = new TupleTag<TableRow>() {
						private static final long serialVersionUID = 1396384401078186092L;
					};
					log.info("BigQuery Table Insertion Start");
					PCollectionTuple results = lines
							.apply(ParDo.withOutputTags(outputRows, TupleTagList.of(invalideItems).and(invalideItemLogs))
									.of(new DoFn<String, TableRow>() {
										
										private static final long serialVersionUID = -5089389898437168768L;
			
										public void processElement(ProcessContext c) {
											List<String> tokens = schemaDef.getSplitter().splitToList(c.element());
											
											StringBuffer errors = new StringBuffer();
											// get first field as Key
											TableRow row = new TableRow();
											boolean headerFlag = false;
											
											for (int i = 0; i < schemaDef.getFields().size()-1; i++) {
												SchemaField cur_field = schemaDef.getFields().get(i);
			
												try {
													String item;
													try {
														item = tokens.get(i);
														if(item.contains(header)){
															headerFlag = true;
															break;
														}
													} catch (IndexOutOfBoundsException e) {
														throw new Exception("Missed");
													}
													Object validatedValue = cur_field.isValidate(item);
													row.set(cur_field.getName(), validatedValue);
												} catch (Exception e) {
													errors.append(
															String.format("[%s] - %s; ", cur_field.getName(), e.getMessage()));
												}
											}
											if(!headerFlag){
											if (errors.length() > 1) {
												
												c.sideOutput(invalideItems, c.element());
												c.sideOutput(invalideItemLogs, SchemaUtils.getErrorLogTableRow(filepath,tokens.get(0),
														c.element(), errors.toString()));
											} else {
												row.set("FILE_NAME_DATE",fileName);
												c.output(row);
											}}
										}
									}).named("ConvertStringToRow"));
					
					log.info("BigQuery Table Insertion End");
					
					log.info("BigQuery Succes table Insertion Start");
					PCollection<TableRow> quotes =results.get(outputRows);
					quotes
					.apply(BigQueryIO.
							Write.named("Write")
							.to(successTable)
							.withSchema(schemaDef.populateTableSchema())
							.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
							.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
					log.info("BigQuery Succes table Insertion End");
					
					log.info("BigQuery Error table Insertion Start");
					log.info("Error List Size" + TupleTagList.of(invalideItems).size());
					
						
						results.get(invalideItemLogs)
						.apply(BigQueryIO.Write.named("WriteToBigQueryErrorLog").to(errorTable)
								.withSchema(SchemaUtils.getErroLogSchema())
								.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
								.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
						log.info("BigQuery Error table Insertion End");
					
					
					results.get(invalideItems)
					.apply(TextIO.Write.named("WriteInvalidItems").to(filepath).withSuffix(".csv"));
			
			
					p.run();
					
					}catch(Exception e){
						throw e;
					}
	}
}

