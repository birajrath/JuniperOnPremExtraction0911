package com.dataextract.dto;

public class SrcSysDto {
	
	String application_user;
	int src_sys_id;
	String src_unique_name;
	String country_code;
	String src_sys_desc;
	String src_extract_type;
	int connection_id;
	String target;
	String tableList;
	String fileList;
	String dataPath;
	String encryptionStatus;
	
	
	
	
	
	
	
	
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	public String getFileList() {
		return fileList;
	}
	public void setFileList(String fileList) {
		this.fileList = fileList;
	}
	public String getTableList() {
		return tableList;
	}
	public void setTableList(String tableList) {
		this.tableList = tableList;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public String getApplication_user() {
		return application_user;
	}
	public void setApplication_user(String application_user) {
		this.application_user = application_user;
	}
	
	public int getSrc_sys_id() {
		return src_sys_id;
	}
	public void setSrc_sys_id(int src_sys_id) {
		this.src_sys_id = src_sys_id;
	}
	public String getSrc_unique_name() {
		return src_unique_name;
	}
	public void setSrc_unique_name(String src_unique_name) {
		this.src_unique_name = src_unique_name;
	}
	public String getCountry_code() {
		return country_code;
	}
	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}
	public String getSrc_sys_desc() {
		return src_sys_desc;
	}
	public void setSrc_sys_desc(String src_sys_desc) {
		this.src_sys_desc = src_sys_desc;
	}
	public String getSrc_extract_type() {
		return src_extract_type;
	}
	public void setSrc_extract_type(String src_extract_type) {
		this.src_extract_type = src_extract_type;
	}
	public int getConnection_id() {
		return connection_id;
	}
	public void setConnection_id(int connection_id) {
		this.connection_id = connection_id;
	}
	public String getEncryptionStatus() {
		return encryptionStatus;
	}
	public void setEncryptionStatus(String encryptionStatus) {
		this.encryptionStatus = encryptionStatus;
	}

}
