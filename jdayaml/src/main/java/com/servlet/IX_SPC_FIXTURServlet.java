package com.servlet;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.process.JDAFileUnzipping;
import com.process.MailUser;
import com.process.StorageService;
import com.utils.Constants;

@SuppressWarnings("serial")
@WebServlet(
	    name = "IX_SPC_FIXTURServlet",
	    urlPatterns = {
	      "/ix_spc_fixtur"})
public class IX_SPC_FIXTURServlet extends HttpServlet implements Constants{
	static Logger log = Logger.getLogger(IX_SPC_FIXTURServlet.class.getName());
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {


		try{
			List<String> fileNameList = StorageService.readFileFromStorage(BUCKETNAME, "IX_SPC_FIXTURE_", "");
			if(fileNameList.size() == 0){		 

				log.info("No Files found");
				MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_FIXTURE Notification", CONTENT); 
			}
			
			for(int i =0 ; i< fileNameList.size(); i++ ){
				StorageService.copyingFilefromPath(BUCKETNAME, BUCKETNAME_ARCHIVE,fileNameList.get(i),"");
				String filepath = "gs://"+ BUCKETNAME   + "/" + fileNameList.get(i);
				String unzipPath = "gs://"+ BUCKETNAME   + "/fixtureProcess/" + fileNameList.get(i).replace(".csv.gz", "");
				JDAFileUnzipping.run( filepath, unzipPath);
				//StorageService.deletingFileFromStorage(BUCKETNAME, fileNameList.get(i));
			}
		}
		catch(Exception e){
			log.info("Exception Occured : " + e.getMessage());
			MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_FIXTURE Notification","Job Failed because of below reason " + e.getMessage());
		}
		
	}
}
