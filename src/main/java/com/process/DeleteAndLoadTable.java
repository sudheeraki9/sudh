package com.process;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
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
import com.utils.Constants;
public class DeleteAndLoadTable implements Constants {
	
	static Logger log = Logger.getLogger(DeleteAndLoadTable.class.getName());
	public static void deleteAndLoadTable(String datasetId,String tableId,String tableId2,String bucketName,String fileName,String query1,String query2,boolean matchedFlag) throws IOException, GeneralSecurityException,Exception {
	
		try{
			GoogleCredential credential = GoogleCredential.getApplicationDefault();


			if (credential.createScopedRequired()) {
				credential =
						credential.createScoped(
								Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
			}

			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
			Bigquery bigqueryService =
					new Bigquery.Builder(httpTransport, jsonFactory, credential)
			.setApplicationName(PROJECT_NAME)
			.build();
				
			// * Project ID of the table to delete
			String projectId = PROJECT_NAME;
			   log.info("Deleting table from BigQuery - Start");
			try{
				Bigquery.Tables.Delete request = bigqueryService.tables().delete(projectId, datasetId, tableId);
				request.execute();

				Tables tableRequest = bigqueryService.tables();
				Table table = tableRequest.get(projectId,datasetId,tableId).execute();
				if(table!= null){
					//throw ;
					 log.info("table not deleted");
				}
				  log.info("Deleting table from BigQuery - End");
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
				
				Bigquery.Jobs.Insert request = bigqueryService.jobs().insert(projectId, job);
				log.info(" Creating table End " + tableId);	
			 	request.execute();
			//returnJob.w	
				Table table = tableCreation(bigqueryService,projectId,datasetId,tableId);
		
				while(table == null){
					Thread.sleep(30000);
					table=tableCreation(bigqueryService,projectId,datasetId,tableId);
				}
				log.info("Table Created succsessfully " + tableId);	
				log.info(" Creating table End " + tableId);	
			}catch(GoogleJsonResponseException ex){
				log.info("Exception at Creating table " + ex.getMessage());			
			}
			catch(Exception ex){
				log.info("Exception at Creating table " + ex.getMessage());			
			}
			
			if(matchedFlag){
				
				
				  log.info("Deleting table from BigQuery - Start");
					try{
						Bigquery.Tables.Delete request = bigqueryService.tables().delete(projectId, datasetId, tableId2);
						request.execute();

						Tables tableRequest = bigqueryService.tables();
						Table table = tableRequest.get(projectId,datasetId,tableId2).execute();
						if(table!= null){
							//throw ;
							 log.info("table not deleted");
						}
						  log.info("Deleting table from BigQuery - End");
					}catch(GoogleJsonResponseException ex){
						

						if(ex.getLocalizedMessage() != null){
							if(ex.getLocalizedMessage().contains("Not found: Table")){
								 log.info("table already deleted");
							}else{
								log.info("Exception at deleting table " + ex.getMessage());
						}}				
					}
					
					try { 
						Job content = new Job();
						
						JobReference jobref = new JobReference();			
						jobref.setProjectId(PROJECT_NAME);
						content.setJobReference(jobref);
						
						JobConfiguration jobconfig = new JobConfiguration();			
						JobConfigurationQuery query = new JobConfigurationQuery();
						query.setQuery(query2);
						query.setPriority("BATCH");
						
						TableReference tableRef = new TableReference();
						tableRef.setDatasetId(datasetId);
						tableRef.setProjectId(PROJECT_NAME);
						tableRef.setTableId(tableId2);
						
						query.setDestinationTable(tableRef);
						query.setAllowLargeResults(true);
						jobconfig.setQuery(query);
					
						content.setConfiguration(jobconfig);	
						
						bigqueryService.jobs().insert(projectId, content).execute();
						
						
						
						Table table = tableCreation(bigqueryService,projectId,datasetId,tableId2);
						
						while(table == null){
							Thread.sleep(30000);
							table=tableCreation(bigqueryService,projectId,datasetId,tableId2);
						}
						log.info("Table Created succsessfully " + tableId2);	
					}catch(GoogleJsonResponseException ex){
						log.info("Exception at Creating table " + ex.getMessage());			
					}
			}
			
			
		}catch(GoogleJsonResponseException ex){
			if(ex.getLocalizedMessage() != null){
				if(ex.getLocalizedMessage().contains("Not found: Table")){
					log.info("table already deleted.waiting for the tasble creation");

				}else{
				throw ex;
			}}
		}
}
	private static Table tableCreation(Bigquery bigqueryService, String projectId, String datasetId, String tableId) throws IOException {
		// TODO Auto-generated method stub
		Table table = null;
		try{
		Tables tableRequest = bigqueryService.tables();
		 table = tableRequest.get(projectId,datasetId,tableId).execute();
		
		}
		catch(GoogleJsonResponseException ex){
			if(ex.getLocalizedMessage() != null){
				if(ex.getLocalizedMessage().contains("Not found: Table")){
					log.info("table already deleted.waiting for the tasble creation");
					return null;
				}else{
				throw ex;
			}}
		}
		return table;
	}
	
}
