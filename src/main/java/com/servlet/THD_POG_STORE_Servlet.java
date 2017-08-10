package com.servlet;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.process.DataFlowProcessForJDA;
import com.process.DeleteErrorTable;
import com.process.MailUser;
import com.process.StorageService;
import com.utils.Constants;
import com.utils.SchemaDef.DelimiterType;

@SuppressWarnings("serial")
@WebServlet(
	    name = "THD_POG_STORE_Servlet",
	    urlPatterns = {
	      "/thd_pog_store"})
public class THD_POG_STORE_Servlet extends HttpServlet implements Constants{
	static Logger log = Logger.getLogger(THD_POG_STORE_Servlet.class.getName());
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		
		try{

			String sampleSchema = "[{\"mode\": \"NULLABLE\",\"name\":\"DBParentPlanogramKey\",\"type\":\"INTEGER\",\"description\": \"DB parent planogram key\"},{\"mode\": \"NULLABLE\",\"name\":\"StoreNumber\",\"type\":\"STRING\",\"description\": \"store number\"},{\"mode\": \"NULLABLE\",\"name\":\"DBstatus\",\"type\":\"INTEGER\",\"description\": \"Status\"},{\"mode\": \"NULLABLE\",\"name\":\"FILE_NAME_DATE\",\"type\":\"STRING\",\"description\": \"file name date\"}]";
			
			 List<String> fileNameList = StorageService.readFileFromStorage(BUCKETNAME, "THD_POG_STORE_", "");
			 log.info("Total files" + fileNameList.size());
			 if(fileNameList.size() == 0){		 
				
					log.info("No Files found");
					MailUser.sendMail(PROJECT_NAME + " : THD_POG_STORE Notification" , CONTENT); 
			 }
			 
			 for(int i =0 ; i<fileNameList.size(); i++ ){
					
					
					String dataformat = fileNameList.get(i).replace("THD_POG_STORE_", "");
					dataformat = dataformat.replace(".csv.gz", "");
					
					String tableSpec = PROJECT_NAME + ":JDA_Tables.THD_POG_STORE";
					String tableErrorSpec  = PROJECT_NAME+":JDA_Error.THD_POG_STORE_ErrorLog";
					
					String successtable = tableSpec + "_" + dataformat;
					String errorTable = tableErrorSpec + "_" + dataformat;

					String filepath = "gs://"+ BUCKETNAME   + "/" + fileNameList.get(i);

					StorageService.copyingFilefromPath(BUCKETNAME,BUCKETNAME_ARCHIVE,  fileNameList.get(i), "");
					DataFlowProcessForJDA.run(sampleSchema,successtable,errorTable,TEMPLOCATION,filepath,"","DBParentPlanogramKey",DelimiterType.PIPE,fileNameList.get(i));		
					DeleteErrorTable.deleteErrorTable("THD_POG_STORE_ErrorLog_" + dataformat, "JDA_Error",errorTable, "THD_POG_STORE");	
					DeleteErrorTable.successTableNotification("THD_POG_STORE_"+ dataformat, "JDA_Tables", successtable, "THD_POG_STORE");					
					StorageService.deletingFileFromStorage(BUCKETNAME, fileNameList.get(i));
					log.info("Dataflow completed for Product");
				}
			}
			catch(Exception e){
				log.info("Exception Occured : " + e.getLocalizedMessage());
				MailUser.sendMail(PROJECT_NAME + " : THD_POG_STORE Notification","Job failed because of below reason : " + e.getMessage()); 
			}
		}
	}	
