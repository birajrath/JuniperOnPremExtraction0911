package com.dataextract.util;

public class ResponseUtil {

	
	public static String createResponse(String status, String message) {
		
		return "{ 'status': '" + status+ "','message':'" +message+"' }";
	}
}
