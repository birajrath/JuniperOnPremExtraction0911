package com.dataextract.fetchdata;

import java.io.IOException;

import com.dataextract.dto.RealTimeExtractDto;

public interface ExtractJava {
	
	
	public String pullData(RealTimeExtractDto rtDataExtract) throws IOException;

}
