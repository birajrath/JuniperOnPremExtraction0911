package com.dataextract.dto;

public class FieldMetadataDto {

	String file_name;
	int field_position;
	String field_name;
	String field_datatype;
	int length;
	
	
	
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	public String getFile_name() {
		return file_name;
	}
	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}
	
	public int getField_position() {
		return field_position;
	}
	public void setField_position(int field_position) {
		this.field_position = field_position;
	}
	public String getField_name() {
		return field_name;
	}
	public void setField_name(String field_name) {
		this.field_name = field_name;
	}
	public String getField_datatype() {
		return field_datatype;
	}
	public void setField_datatype(String field_datatype) {
		this.field_datatype = field_datatype;
	}
	
	
}
