package com.dataextract.dto;

public class TargetDto {
	
	
	String target_unique_name;
	String target_type;
	String target_project;
	String service_account;
	String target_bucket;
	String target_knox_url;
	String target_user;
	String target_password;
	String target_hdfs_path;
	int target_id;
	
	
	
	
	public int getTarget_id() {
		return target_id;
	}
	public void setTarget_id(int target_id) {
		this.target_id = target_id;
	}
	public String getTarget_knox_url() {
		return target_knox_url;
	}
	public void setTarget_knox_url(String target_knox_url) {
		this.target_knox_url = target_knox_url;
	}
	public String getTarget_user() {
		return target_user;
	}
	public void setTarget_user(String target_user) {
		this.target_user = target_user;
	}
	public String getTarget_password() {
		return target_password;
	}
	public void setTarget_password(String target_password) {
		this.target_password = target_password;
	}
	public String getTarget_hdfs_path() {
		return target_hdfs_path;
	}
	public void setTarget_hdfs_path(String target_hdfs_path) {
		this.target_hdfs_path = target_hdfs_path;
	}
	public String getTarget_unique_name() {
		return target_unique_name;
	}
	public void setTarget_unique_name(String target_unique_name) {
		this.target_unique_name = target_unique_name;
	}
	public String getTarget_type() {
		return target_type;
	}
	public void setTarget_type(String target_type) {
		this.target_type = target_type;
	}
	public String getTarget_project() {
		return target_project;
	}
	public void setTarget_project(String target_project) {
		this.target_project = target_project;
	}
	public String getService_account() {
		return service_account;
	}
	public void setService_account(String service_account) {
		this.service_account = service_account;
	}
	public String getTarget_bucket() {
		return target_bucket;
	}
	public void setTarget_bucket(String target_bucket) {
		this.target_bucket = target_bucket;
	}
	
	

}
