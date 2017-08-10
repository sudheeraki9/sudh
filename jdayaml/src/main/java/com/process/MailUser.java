package com.process;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.simple.JSONObject;

import com.utils.Constants;



public class MailUser implements Constants { 
	static Logger log = Logger.getLogger(MailUser.class.getName());
  public static void sendMail(String subject,String message){

	    try { 
	    	
	      HttpClient httpClient = new HttpClient();
	      StringRequestEntity requestEntity = new StringRequestEntity(
	        String.format(
	          "{" +
	            "\"channel\": \"#hdcan-analytics\", " +
	            "\"username\": \"%s\", " +
	            "\"text\": \"%s\", " +
	            "\"icon_emoji\": \":heavy_exclamation_mark:\"" +
	          "}",
	          subject,
	          JSONObject.escape(message)),
	        "application/json",
	        "UTF-8");

	      PostMethod postMethod = new PostMethod(SLACK_POST_URL);
	      postMethod.setRequestEntity(requestEntity);

	      int statusCode = httpClient.executeMethod(postMethod);
	      if (statusCode != 200) {
	        log.info("Slack message error: Invalid status code received: " + statusCode);
	      }
	    } catch (IOException e) {
	      log.info("Slack message exception: " + e.getMessage());
	    }
	  }
}
