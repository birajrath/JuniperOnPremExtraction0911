package com.dataextract.dto;

public class FileMetadataDto {
	
	
	int file_sequence;
	String file_path;
	String file_name;
	String file_type;
	String file_delimiter;
	String header_count;
	String trailer_count;
	String field_list;
	String field_type_list;
	String avro_conv_flag;
	String bus_dt_format;
	String bus_dt_loc;
	int bus_dt_start;
	String count_loc;
	int count_start;
	int count_legnth;
	
	
	
	
	
	
	public String getFile_path() {
		return file_path;
	}

	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}

	public int getFile_sequence() {
		return file_sequence;
	}

	public void setFile_sequence(int file_sequence) {
		this.file_sequence = file_sequence;
	}

	public String getField_type_list() {
		return field_type_list;
	}

	public void setField_type_list(String field_type_list) {
		this.field_type_list = field_type_list;
	}

	public String getField_list() {
		return field_list;
	}

	public void setField_list(String field_list) {
		this.field_list = field_list;
	}

	
	

	public String getBus_dt_format() {
		return bus_dt_format;
	}

	public void setBus_dt_format(String bus_dt_format) {
		this.bus_dt_format = bus_dt_format;
	}

	

	public String getBus_dt_loc() {
		return bus_dt_loc;
	}

	public void setBus_dt_loc(String bus_dt_loc) {
		this.bus_dt_loc = bus_dt_loc;
	}

	public int getBus_dt_start() {
		return bus_dt_start;
	}

	public void setBus_dt_start(int bus_dt_start) {
		this.bus_dt_start = bus_dt_start;
	}

	public String getCount_loc() {
		return count_loc;
	}

	public void setCount_loc(String count_loc) {
		this.count_loc = count_loc;
	}

	public int getCount_start() {
		return count_start;
	}

	public void setCount_start(int count_start) {
		this.count_start = count_start;
	}

	public int getCount_legnth() {
		return count_legnth;
	}

	public void setCount_legnth(int count_legnth) {
		this.count_legnth = count_legnth;
	}

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public String getFile_type() {
		return file_type;
	}

	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}

	public String getFile_delimiter() {
		return file_delimiter;
	}

	public void setFile_delimiter(String file_delimiter) {
		this.file_delimiter = file_delimiter;
	}

	public String getHeader_count() {
		return header_count;
	}

	public void setHeader_count(String header_count) {
		this.header_count = header_count;
	}

	public String getTrailer_count() {
		return trailer_count;
	}

	public void setTrailer_count(String trailer_count) {
		this.trailer_count = trailer_count;
	}


	public String getAvro_conv_flag() {
		return avro_conv_flag;
	}

	public void setAvro_conv_flag(String avro_conv_flag) {
		this.avro_conv_flag = avro_conv_flag;
	}

	
	
	

}
