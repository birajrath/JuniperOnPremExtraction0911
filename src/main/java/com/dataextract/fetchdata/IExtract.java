package com.dataextract.fetchdata;




//import java.io.IOException;

//import org.springframework.expression.ParseException;
//import org.springframework.http.HttpEntity;


import com.dataextract.dto.RealTimeExtractDto;


public interface IExtract {
	
	
	
	public  String callNifiDataRealTime(RealTimeExtractDto extractDto,String conn_string,String date,Long runId) throws UnsupportedOperationException, Exception;

	public String callNifiUnixRealTime(RealTimeExtractDto rtExtractDto, String date, Long runId) throws UnsupportedOperationException, Exception;

	public String callNifiHadoopRealTime(RealTimeExtractDto rtExtractDto, String date, Long runId) throws UnsupportedOperationException, Exception;

	public String callNifiHivePropagateRealTime(RealTimeExtractDto rtExtractDto, String date, Long runId) throws UnsupportedOperationException, Exception;
	
}
