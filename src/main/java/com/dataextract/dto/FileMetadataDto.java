package com.dataextract.dto;

public class FileMetadataDto {
	
	
	
	String file_name;
	String file_type;
	String file_delimiter;
	String header_count;
	String trailer_count;
	String field_list;
	String avro_conv_flag;
	String application_user;
	
	
	
	
	
	public String getField_list() {
		return field_list;
	}

	public void setField_list(String field_list) {
		this.field_list = field_list;
	}

	public String getApplication_user() {
		return application_user;
	}

	public void setApplication_user(String application_user) {
		this.application_user = application_user;
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
