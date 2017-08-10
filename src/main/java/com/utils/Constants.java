package com.utils;

import java.util.List;

public interface Constants {
	
	
	String YESTERDAY_DATE= CommonUtis.retunYesDate();

	// String sampleSchema = "[{\"name\":\"DATE\",\"type\":\"STRING\",\"mode\":\"NULLABLE\"},{\"name\":\"COUNT\",\"type\":\"STRING\",\"mode\":\"NULLABLE\"},{\"name\":\"PROMO_WEEK\",\"type\":\"STRING\",\"mode\":\"NULLABLE\"},{\"name\":\"BEG_MONTH\",\"type\":\"STRING\",\"mode\":\"NULLABLE\"},{\"name\":\"END_MONTH\",\"type\":\"STRING\",\"mode\":\"NULLABLE\"}]";
	CrunchifyGetPropertyValues values = new CrunchifyGetPropertyValues();
	List<String> propValues = values.getPropValues(); 
	
	
	String PROJECT_NAME = propValues.get(0);
	
	String SLACK_POST_URL = "https://hooks.slack.com/services/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";
	String BUCKETNAME = PROJECT_NAME.contains("pr")?propValues.get(3):propValues.get(1);
	String BUCKETNAME_ARCHIVE = PROJECT_NAME.contains("pr")?propValues.get(4):propValues.get(2);	
	
	String TEMPLOCATION = "gs://" + BUCKETNAME +"/Staging";
	//String TEMPLOCATION = "gs://" + BUCKETNAME;
	
	String CONTENT = "No Files found in the Bucket :" + BUCKETNAME ;
			
			
	
}
