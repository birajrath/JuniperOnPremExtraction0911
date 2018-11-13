package com.dataextract.dto;

import java.util.ArrayList;

public class FileInfoDto {
	
	
	int src_sys_id;
	String dataPath;
	ArrayList<FileMetadataDto> fileMetadataArr;
	ArrayList<FieldMetadataDto> fieldMetadataArr;
	
	
	
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	public int getSrc_sys_id() {
		return src_sys_id;
	}
	public void setSrc_sys_id(int src_sys_id) {
		this.src_sys_id = src_sys_id;
	}
	public ArrayList<FileMetadataDto> getFileMetadataArr() {
		return fileMetadataArr;
	}
	public void setFileMetadataArr(ArrayList<FileMetadataDto> fileMetadataArr) {
		this.fileMetadataArr = fileMetadataArr;
	}
	public ArrayList<FieldMetadataDto> getFieldMetadataArr() {
		return fieldMetadataArr;
	}
	public void setFieldMetadataArr(ArrayList<FieldMetadataDto> fieldMetadataArr) {
		this.fieldMetadataArr = fieldMetadataArr;
	}
	
	

}
