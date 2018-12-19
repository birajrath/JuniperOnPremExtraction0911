package com.dataextract.dto;

public class FeedDto {
	
	String juniper_user;
	int feed_id;
	String feed_name;
	String country_code;
	String feed_desc;
	String feed_extract_type;
	int src_conn_id;
	String target;
	String tableList;
	String fileList;
	String dbList;
	String filePath;
	String encryptionStatus;
	String project;
	int project_sequence;
	
	
	
	public String getDbList() {
		return dbList;
	}
	public void setDbList(String dbList) {
		this.dbList = dbList;
	}
	public int getProject_sequence() {
		return project_sequence;
	}
	public void setProject_sequence(int project_sequence) {
		this.project_sequence = project_sequence;
	}
	public String getProject() {
		return project;
	}
	public void setProject(String project) {
		this.project = project;
	}
	public int getFeed_id() {
		return feed_id;
	}
	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}
	public String getFeed_name() {
		return feed_name;
	}
	public void setFeed_name(String feed_name) {
		this.feed_name = feed_name;
	}
	public String getFeed_desc() {
		return feed_desc;
	}
	public void setFeed_desc(String feed_desc) {
		this.feed_desc = feed_desc;
	}
	public String getFeed_extract_type() {
		return feed_extract_type;
	}
	public void setFeed_extract_type(String feed_extract_type) {
		this.feed_extract_type = feed_extract_type;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
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
	
	
	
	public String getJuniper_user() {
		return juniper_user;
	}
	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}
	public String getCountry_code() {
		return country_code;
	}
	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}
	
	
	public int getSrc_conn_id() {
		return src_conn_id;
	}
	public void setSrc_conn_id(int src_conn_id) {
		this.src_conn_id = src_conn_id;
	}
	public String getEncryptionStatus() {
		return encryptionStatus;
	}
	public void setEncryptionStatus(String encryptionStatus) {
		this.encryptionStatus = encryptionStatus;
	}
}
