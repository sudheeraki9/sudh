package com.process;

import java.util.logging.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.utils.Constants;

public class JDAFileUnzipping implements Constants {
	static Logger log = Logger.getLogger(JDAFileUnzipping.class.getName());
	
	public static void run(final String filepath,String unzipPath) throws Exception {
	
				try{
									
					DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
					options.setRunner(BlockingDataflowPipelineRunner.class);
					options.setProject(PROJECT_NAME);
					options.setTempLocation(TEMPLOCATION);
					options.setStagingLocation(TEMPLOCATION);
					
					
					//PipelineOptions options = PipelineOptionsFactory.create();
					
					// Then create the pipeline.
					Pipeline p = Pipeline.create(options);
					log.info("PipeLine Flow reading file from Storage Start");
					PCollection<String> lines = p.apply(TextIO.Read.named("ReadDataFromStorage")
							.from(filepath)
							.withCompressionType(TextIO.CompressionType.GZIP)); //gs://sap_thd/Promo_Week.csv"));
					lines.apply(TextIO.Write.named("Unzipping File Contents").to(unzipPath).withSuffix(".csv"));
					
					p.run();
					
					}catch(Exception e){
						throw e;
					}
	}
}

