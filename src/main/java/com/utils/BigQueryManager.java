package com.utils;

// Library files
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;


/**
 * Class for managing API calls to Big Query.
 * @author PXB8449
 *
 */
public class BigQueryManager {

  /**
   * Logging tool.
   */
  private static Logger log = Logger.getLogger(BigQueryManager.class.getName());


  /**
   * Delete and load a table.
   *
   * @param datasetId - The data set to make the changes inn.
   * @param tableId - The name of the table to delete and create.
   * @param queryExec - The query to execute when creating the table.
   *
   * @throws IOException
   * @throws GeneralSecurityException
   * @throws Exception
   */
  public static void deleteAndCreateTable(
    String datasetId,
    String tableId,
    String queryExec)
      throws IOException, GeneralSecurityException, Exception {
    
    Bigquery bigqueryService = createService();
    
    // Delete the table first.
    log.info(String.format(
      "\nDeleting the table \"%s\"",
      tableId));
    deleteTablePrivate(bigqueryService, datasetId, tableId);
    
    // Recereate the table.
    log.info(String.format(
      "\nCreating the table \"%s\" using the query \"%s\"",
      tableId,
      queryExec));
    createTable(bigqueryService, datasetId, tableId, queryExec);
  }
  
  /**
   * Check if data has been properly inserted into the table.
   *
   * @param tableName - The name of the table to check for.
   * @param datasetId - The data set containing the table.
   * @param tableId - The ID of the table.
   */
  public static boolean checkIfInserted(String datasetId, String tableId) {
    try {
      Bigquery bigquery = createService();
      List<TableRow> list = getSampleQuery(bigquery, tableId);
      
      if (list == null) {
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Delete the specified table.
   *
   * @param datasetId - The ID of the dataset to delete the table from.
   * @param tableId - The ID of the table to delete.
   *
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public static void deleteTable(
    String datasetId,
    String tableId) throws IOException, GeneralSecurityException {
    
    Bigquery bigqueryService = createService();
    deleteTablePrivate(bigqueryService, datasetId, tableId);
  }
  
  /**
   * Function for deleting a table from BigQuery.
   *
   * @param bigqueryService - The BigQuery service to use.
   * @param datasetId - Name of data set to delete from.
   * @param tableId - ID of the table to delete.
   *
   * @throws IOException 
   */
  private static void deleteTablePrivate(
    Bigquery bigqueryService,
    String datasetId,
    String tableId) throws IOException {
    
    log.info(String.format(
      "Deleting the table \"%s\" from BigQuery",
      tableId));

    try {
      
      // Run the delete request.
      Bigquery.Tables.Delete request = bigqueryService.tables().
        delete(Constants.PROJECT_NAME, datasetId, tableId);
      request.execute();

      // Verify that the table is deleted.
      Tables tableRequest = bigqueryService.tables();
      Table table = tableRequest.get(Constants.PROJECT_NAME, datasetId, tableId).execute();

      if (table != null) {
        log.warning(String.format(
          "\nThe table \"%s\" could not be deleted from BigQuery",
          tableId));
      }
      log.info(String.format(
        "\nThe table \"%s\" has been successfully deleted from BigQuery",
        tableId));

    } catch (GoogleJsonResponseException ex) {

      if (ex.getLocalizedMessage() != null) {
        if (ex.getLocalizedMessage().contains("Not found: Table")) {
          log.warning(String.format(
            "\nThe table \"%s\" has already been deleted from BigQuery",
            tableId));
        } else {
          log.severe(String.format(
            "\nAn exception occurred when deleteing the table \"%s\"\nError: \"%s\"",
            tableId,
            ex.getMessage()));
        }
      }
    }
  }
  
  /**
   * Create a table in BigQuery.
   *
   * @param bigqueryService - The BigQuery service to use.
   * @param datasetId - Name of data set to delete from.
   * @param tableId - ID of the table to delete.
   * @param queryExec - Query to execute during table creation.
   *
   * @throws IOException
   * @throws GeneralSecurityException
   * @throws Exception
   */
  private static void createTable(    
    Bigquery bigqueryService,
    String datasetId,
    String tableId,
    String queryExec) throws IOException, GeneralSecurityException, Exception {

    log.info(String.format(
      " Creating the table with the ID \"%s\"", 
      tableId));
    try {

      Job job = new Job();
      
      // Setup the job reference.
      JobReference jobref = new JobReference();
      jobref.setProjectId(Constants.PROJECT_NAME);
      job.setJobReference(jobref);

      // Setup the table reference.
      TableReference tableRef = new TableReference();
      tableRef.setDatasetId(datasetId);
      tableRef.setProjectId(Constants.PROJECT_NAME);
      tableRef.setTableId(tableId);

      // Setup the query.
      JobConfigurationQuery query = new JobConfigurationQuery();
      query.setQuery(queryExec);
      query.setDestinationTable(tableRef);
      query.setAllowLargeResults(true);
      
      // Setup the job configuration.
      JobConfiguration jobconfig = new JobConfiguration();
      jobconfig.setQuery(query);
      job.setConfiguration(jobconfig);

      // Run the insert request.
      Bigquery.Jobs.Insert request = bigqueryService.jobs().
        insert(Constants.PROJECT_NAME, job);
      log.info(String.format(
        "The table with the ID \"%s\" has been deleted.",
        tableId));
      request.execute();

      // Check that the table was created.
      Table table = fetchTable(bigqueryService, datasetId, tableId);
      int rtyCount = 0;
      while (table == null) {
        Thread.sleep(1000);
        table = fetchTable(bigqueryService, datasetId, tableId);
        rtyCount++;
        if(rtyCount == 10) {

          String errMsg = String.format(
            "Could not created the table named \"%s\" for the dataset \"%s\"",
            tableId,
            datasetId);

          log.severe(errMsg);
          throw new InstantiationException(errMsg);
        }
      }

      log.info(String.format(
        "The table \"%s\" was created succsessfully",
        tableId));
    } catch (GoogleJsonResponseException ex) {
      log.severe(String.format(
        "An exception occurred when creating the table \"%s\"\nError: \"%s\"",
        tableId,
        ex.getMessage()));
    } catch (Exception ex) {
      log.severe(String.format(
        "An exception occurred when creating the table \"%s\"\nError: \"%s\"",
        tableId,
        ex.getMessage()));
   }
 }

  /**
   * Helper function for fetching a table from BigQuery.
   */
  private static Table fetchTable(
    Bigquery bigqueryService,
    String datasetId,
    String tableId) throws IOException {

    Table table = null;
    try {
      table = bigqueryService.tables().
        get(Constants.PROJECT_NAME, datasetId, tableId).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getLocalizedMessage() != null) {
        if (e.getLocalizedMessage().contains("Not found: Table")) {
          log.info(String.format("\tTable: \"%s\" not found.", tableId));
          return null;
        } else {
          throw e;
        }
      }
    }
    
    // Return the table if found.
    return table;
  }
  
  /**
   * Helper function for running a sample query.
   * 
   * @param bigquery - The Big Query service to use.
   * @param tableId - The Table ID to run the query for.
   * @return - Results of the query.
   * @throws IOException
   */
  private static List<TableRow> getSampleQuery(
    Bigquery bigquery,
    String tableId)
    throws IOException {

    String query = "SELECT * FROM [" + tableId + "] LIMIT 10";
    return executeQuery(query, bigquery);
  }
  
  /**
   * Helper function for executing a query.
   *
   * @param querySql - The query to execute.
   * @param bigquery - The Big Query service to use.
   * 
   * @return - List of rows returned by the query.
   *
   * @throws IOException
   */
  private static List<TableRow> executeQuery(
    String querySql, Bigquery bigquery) throws IOException {
    
    QueryResponse query = bigquery.jobs().query(
      Constants.PROJECT_NAME,
      new QueryRequest().setQuery(querySql)).execute();

    JobReference ref = query.getJobReference();
    GetQueryResultsResponse queryResult = bigquery.jobs()
      .getQueryResults(
        ref.getProjectId(),
        ref.getJobId()).execute();

    return queryResult.getRows();
  }
  
  /**
   * Return the Big Query service API.
   *
   * @return - The Big Query service API.
   * @throws IOException 
   * @throws GeneralSecurityException 
   */
  private static Bigquery createService()
    throws IOException, GeneralSecurityException {
    
    // Get the credentials.
    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential = credential
        .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    // Initialize the service.
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    Bigquery bigquery = new Bigquery.Builder(httpTransport, jsonFactory, credential)
      .setApplicationName(Constants.PROJECT_NAME).build();
    
    // Return the service.
    return bigquery;
  }
}
