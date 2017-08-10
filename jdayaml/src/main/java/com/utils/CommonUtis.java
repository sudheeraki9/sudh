package com.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class CommonUtis {
	
	public static String retunYesDate() {
		
		
		 DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -1);
			return dateFormat.format(cal.getTime());
			


	}
}
