package com.dataextract.dto;

public class TargetDto {
	
	
	String target_unique_name;
	String target_type;
	String target_project;
	String service_account;
	String target_bucket;
	String target_knox_host;
	int target_knox_port;
	String target_hdfs_gateway;
	String materialization_flag;
	String partition_flag;
	String hive_gateway;
	String target_user;
	String target_password;
	String target_hdfs_path;
	int target_id;
	String system;
	String drive_id;
	String data_path;
	String full_path;
	String project;
	String juniper_user;
	byte[] encrypted_password;
	byte[] encrypted_key;
	String knox_gateway;
	String trust_store_path;
	String trust_store_password;
	byte[] encrypted_trust_store_password;
	String service_account_key;
	
	
	
	
	
	
	public String getService_account_key() {
		return service_account_key;
	}
	public void setService_account_key(String service_account_key) {
		this.service_account_key = service_account_key;
	}
	public String getTarget_knox_host() {
		return target_knox_host;
	}
	public void setTarget_knox_host(String target_knox_host) {
		this.target_knox_host = target_knox_host;
	}
	
	public int getTarget_knox_port() {
		return target_knox_port;
	}
	public void setTarget_knox_port(int target_knox_port) {
		this.target_knox_port = target_knox_port;
	}
	public String getTarget_hdfs_gateway() {
		return target_hdfs_gateway;
	}
	public void setTarget_hdfs_gateway(String target_hdfs_gateway) {
		this.target_hdfs_gateway = target_hdfs_gateway;
	}
	public String getMaterialization_flag() {
		return materialization_flag;
	}
	public void setMaterialization_flag(String materialization_flag) {
		this.materialization_flag = materialization_flag;
	}
	public String getPartition_flag() {
		return partition_flag;
	}
	public void setPartition_flag(String partition_flag) {
		this.partition_flag = partition_flag;
	}
	public String getHive_gateway() {
		return hive_gateway;
	}
	public void setHive_gateway(String hive_gateway) {
		this.hive_gateway = hive_gateway;
	}
	public byte[] getEncrypted_password() {
		return encrypted_password;
	}
	public void setEncrypted_password(byte[] encrypted_password) {
		this.encrypted_password = encrypted_password;
	}
	public byte[] getEncrypted_key() {
		return encrypted_key;
	}
	public void setEncrypted_key(byte[] encrypted_key) {
		this.encrypted_key = encrypted_key;
	}
	public String getProject() {
		return project;
	}
	public void setProject(String project) {
		this.project = project;
	}
	
	
	public String getJuniper_user() {
		return juniper_user;
	}
	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}
	public String getFull_path() {
		return full_path;
	}
	public void setFull_path(String full_path) {
		this.full_path = full_path;
	}
	public int getTarget_id() {
		return target_id;
	}
	public void setTarget_id(int target_id) {
		this.target_id = target_id;
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
	public String getSystem() {
		return system;
	}
	public void setSystem(String system) {
		this.system = system;
	}
	public String getDrive_id() {
		return drive_id;
	}
	public void setDrive_id(String drive_id) {
		this.drive_id = drive_id;
	}
	public String getData_path() {
		return data_path;
	}
	public void setData_path(String data_path) {
		this.data_path = data_path;
	}
	public String getKnox_gateway() {
		return knox_gateway;
	}
	public void setKnox_gateway(String knox_gateway) {
		this.knox_gateway = knox_gateway;
	}
	public String getTrust_store_path() {
		return trust_store_path;
	}
	public void setTrust_store_path(String trust_store_path) {
		this.trust_store_path = trust_store_path;
	}
	public String getTrust_store_password() {
		return trust_store_password;
	}
	public void setTrust_store_password(String trust_store_password) {
		this.trust_store_password = trust_store_password;
	}
	public byte[] getEncrypted_trust_store_password() {
		return encrypted_trust_store_password;
	}
	public void setEncrypted_trust_store_password(byte[] encrypted_trust_store_password) {
		this.encrypted_trust_store_password = encrypted_trust_store_password;
	}
}
