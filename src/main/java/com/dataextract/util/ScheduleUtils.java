package com.dataextract.util;

import com.dataextract.dto.ConnectionDto;
import com.dataextract.dto.DataExtractDto;

public interface ScheduleUtils {
	
	public String composerDataExtractDagCreator(ConnectionDto connDto,DataExtractDto extractDto,String connectionUrl,Long runId,String date,String dagLocation);
	

}
