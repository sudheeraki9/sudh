package com.process;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.ViewDefinition;
import com.utils.Constants;

public class BigQueryService implements Constants {
	public static Bigquery bigQueryservice = serviceIniation();
	static Logger log = Logger.getLogger(BigQueryService.class.getName());
	
	/**
	 * This is to update view based on sales date
	 * @param currentDate
	 * @param viewName : View Name in BigQuery
	 * @param fileName ": Storage File Name
	 * @throws IOException
	 * @throws GeneralSecurityException
	 * @throws Exception
	 */
	public static void updateView(String query,String viewName) throws IOException, GeneralSecurityException,Exception {
		log.info("View Update Start");
		try{
			
			Table content= new Table();    
			TableReference tableReference= new TableReference();
			tableReference.setTableId(viewName);
			tableReference.setDatasetId("RW_Views");
			tableReference.setProjectId(PROJECT_NAME);
			content.setTableReference(tableReference);
			
		
				ViewDefinition view= new ViewDefinition();
				view.setQuery(query);
				content.setView(view);
				bigQueryservice.tables().update(PROJECT_NAME, "RW_Views",viewName, content).execute();
				log.info("View Update Start");	
			
		}
	
		catch(GoogleJsonResponseException ex){
			if(ex.getLocalizedMessage() != null){
				if(ex.getLocalizedMessage().contains("Not found: Table")){					
				}else{
					log.info("View Update Exception" + ex.getMessage());	
				throw ex;
			}}
		}
}
	/**
	 * This is to delete and Create table from View
	 * @param datasetId
	 * @param tableId
	 * @param query1
	 * @throws IOException
	 * @throws GeneralSecurityException
	 * @throws Exception
	 */
	public static void deleteAndLoadTable(String datasetId,String tableId,String query1) throws IOException, GeneralSecurityException,Exception {		
		try{		
			log.info("Deleting table from BigQuery - Start :" + tableId);
			try{
				Bigquery.Tables.Delete request = bigQueryservice.tables().delete(PROJECT_NAME, datasetId, tableId);
				request.execute();

				Tables tableRequest = bigQueryservice.tables();
				Table table = tableRequest.get(PROJECT_NAME,datasetId,tableId).execute();
				if(table!= null){
					log.info("table not deleted");
				}
				log.info("Deleting table from BigQuery - End :" + tableId);
			}catch(GoogleJsonResponseException ex){
				if(ex.getLocalizedMessage() != null){
					if(ex.getLocalizedMessage().contains("Not found: Table")){
						log.info("table already deleted");
					}else{
						log.info("Exception at deleting table " + ex.getMessage());
					}}				
			}
			log.info(" Creating table " + tableId);	
			try { 
				Job job = new Job();

				JobReference jobref = new JobReference();			
				jobref.setProjectId(PROJECT_NAME);
				job.setJobReference(jobref);

				JobConfiguration jobconfig = new JobConfiguration();			
				JobConfigurationQuery query = new JobConfigurationQuery();
				query.setQuery(query1);
				query.setPriority("BATCH");

				TableReference tableRef = new TableReference();
				tableRef.setDatasetId(datasetId);
				tableRef.setProjectId(PROJECT_NAME);
				tableRef.setTableId(tableId);

				query.setDestinationTable(tableRef);
				query.setAllowLargeResults(true);
				jobconfig.setQuery(query);

				job.setConfiguration(jobconfig);	

				Bigquery.Jobs.Insert request = bigQueryservice.jobs().insert(PROJECT_NAME, job);

				Job jobStatus = ((Job)request.execute());
				String status = jobStatus.getStatus().getState();
				jobStatus.getStatus().getErrors();
				while(!status.equalsIgnoreCase("DONE")) {
					status = bigQueryservice.jobs().get(PROJECT_NAME, jobStatus.getId().replace(PROJECT_NAME +":", "")).execute().getStatus().getState();
					if(status.equals("DONE")&& bigQueryservice.jobs().get(PROJECT_NAME, jobStatus.getId().replace(PROJECT_NAME +":", "")).execute().getStatus().getErrors() != null){
						String error = String.valueOf(bigQueryservice.jobs().get(PROJECT_NAME, jobStatus.getId().replace(PROJECT_NAME +":", "")).execute().getStatus().getErrors().toString());
						log.info("error: " + error);
					}
					//System.out.println("Status: " + status);
					TimeUnit.SECONDS.sleep(15);
				}
				log.info("Status of BigQuery Job" + status);

				log.info("Table Created succsessfully " + tableId);	
				log.info(" Creating table End " + tableId);	
			}catch(GoogleJsonResponseException ex){
				log.info("Exception at Creating table " + ex.getMessage());			
			}
			catch(Exception ex){
				log.info("Exception at Creating table " + ex.getMessage());			
			}


		}catch(GoogleJsonResponseException ex){
			if(ex.getLocalizedMessage() != null){
				if(ex.getLocalizedMessage().contains("Not found: Table")){
					log.info("table already deleted");
				}else{
				throw ex;
			}}
		}
}
	
	
	
	/**
	 * BigQuery service initiation
	 * @return BigQuery
	 */
	private static Bigquery serviceIniation() {
		Bigquery bigqueryService = null;
		try{
			GoogleCredential credential = GoogleCredential.getApplicationDefault();


			if (credential.createScopedRequired()) {
				credential =
						credential.createScoped(
								Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
			}

			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
			bigqueryService =
					new Bigquery.Builder(httpTransport, jsonFactory, credential)
			.setApplicationName(PROJECT_NAME)
			.build();
		}
		catch(Exception e){
			log.info("Big Query failed to initalize");
		}
		return bigqueryService;
	}

}
