package com.process;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableRow;
import com.utils.Constants;
public class DeleteErrorTable implements Constants {
	static Logger log = Logger.getLogger(DeleteErrorTable.class.getName());

	public static void deleteErrorTable(String tableName,String datasetId,String tableId,String fileName) 
	{

		try{

			Bigquery bigquery =  BigqueryServiceFactory.getService();

			List<TableRow> list = executeQuery("SELECT * FROM [" + tableId + "]" ,bigquery,PROJECT_NAME);

			if(list == null){

				bigquery.tables().delete(PROJECT_NAME,
						datasetId,
						tableName).execute();
				log.info("Table deleted succesfully because of empty data");
			}else{
				MailUser.sendMail(PROJECT_NAME + ":" + fileName + "notification",
						"Errors were logged for the file " + tableName.replace("_ErrorLog", "")  + ".csv.gz in the table "   + tableName + "\n Number of error records :" + list.size());
						
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void successTableNotification(String tableName,String datasetId,String tableId, String fileName) 
	{

		try{

			Bigquery bigquery =  BigqueryServiceFactory.getService();

			List<TableRow> list = executeQuery("SELECT * FROM [" + tableId + "] LIMIT 10" ,bigquery,PROJECT_NAME);

			if(list == null)
			{	
				
				MailUser.sendMail(PROJECT_NAME + ":" + fileName + "notification",  tableName + "Table Created with no entries inserted" );
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
}
	
	  public static List<TableRow> executeQuery(String querySql, Bigquery bigquery, String projectId)
			    throws IOException {
			  QueryResponse query =
			      bigquery.jobs().query(projectId, new QueryRequest().setQuery(querySql)).execute();

			  // Execute it
			  GetQueryResultsResponse queryResult =
			      bigquery
			          .jobs()
			          .getQueryResults(
			              query.getJobReference().getProjectId(), query.getJobReference().getJobId())
			          .execute();

			  return queryResult.getRows();
			}

}
