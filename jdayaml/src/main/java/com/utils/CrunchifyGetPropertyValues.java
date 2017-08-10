package com.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class CrunchifyGetPropertyValues {
	String result = "";
	InputStream inputStream;
 
	public List<String> getPropValues()  {
		List<String> values = null;
		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";
 
			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
 
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
 
		values = new ArrayList<String>();
 
			// get the property value and print it out
			values.add(prop.getProperty("projectId"));
			values.add(prop.getProperty("bucketPath"));
			values.add( prop.getProperty("archivepath"));
			values.add(prop.getProperty("bucketPath_pr"));
			values.add(prop.getProperty("archivepath_pr"));
			
			
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return values;
	}
}
