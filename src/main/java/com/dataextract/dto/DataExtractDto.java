package com.dataextract.dto;

import java.util.ArrayList;
import java.util.Map;

public class DataExtractDto {

	String application_user;
	String service_account_name;
	int connectionId;
	String country_code;
	int src_sys_id;
	String src_unique_name;
	String src_sys_desc;
	String src_extract_type;
	ArrayList<Map<String,String>> tableInfo=new ArrayList<Map<String,String>>();
	
	String incr_load_tables;
	String full_load_tables;
	String incr_columns;
	String project;
	String bucket;
	String reservior_name;
	int reservoir_id;
	String reservoir_loc;
	String cron;

	
	
	
	public int getReservoir_id() {
		return reservoir_id;
	}
	public void setReservoir_id(int reservoir_id) {
		this.reservoir_id = reservoir_id;
	}
	public ArrayList<Map<String, String>> getTableInfo() {
		return tableInfo;
	}
	public void setTableInfo(ArrayList<Map<String, String>> tableInfo) {
		this.tableInfo = tableInfo;
	}
	public String getSrc_sys_desc() {
		return src_sys_desc;
	}
	public void setSrc_sys_desc(String src_sys_desc) {
		this.src_sys_desc = src_sys_desc;
	}
	public String getReservoir_loc() {
		return reservoir_loc;
	}
	public void setReservoir_loc(String reservoir_loc) {
		this.reservoir_loc = reservoir_loc;
	}
	
	public String getCountry_code() {
		return country_code;
	}
	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}
	public int getConnectionId() {
		return connectionId;
	}
	public void setConnectionId(int connectionId) {
		this.connectionId = connectionId;
	}
	public int getSrc_sys_id() {
		return src_sys_id;
	}
	public void setSrc_sys_id(int src_sys_id) {
		this.src_sys_id = src_sys_id;
	}
	public String getProject() {
		return project;
	}
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public void setProject(String project) {
		this.project = project;
	}
	
	public String getApplication_user() {
		return application_user;
	}
	public void setApplication_user(String application_user) {
		this.application_user = application_user;
	}
	public String getService_account_name() {
		return service_account_name;
	}
	public void setService_account_name(String service_account_name) {
		this.service_account_name = service_account_name;
	}
	
	
	
	
	public String getSrc_extract_type() {
		return src_extract_type;
	}
	public void setSrc_extract_type(String src_extract_type) {
		this.src_extract_type = src_extract_type;
	}

	public String getCron() {
		return cron;
	}
	public void setCron(String cron) {
		this.cron = cron;
	}
	public String getSrc_unique_name() {
		return src_unique_name;
	}
	public void setSrc_unique_name(String src_unique_name) {
		this.src_unique_name = src_unique_name;
	}
	public String getIncr_load_tables() {
		return incr_load_tables;
	}
	public void setIncr_load_tables(String incr_load_tables) {
		this.incr_load_tables = incr_load_tables;
	}
	public String getFull_load_tables() {
		return full_load_tables;
	}
	public void setFull_load_tables(String full_load_tables) {
		this.full_load_tables = full_load_tables;
	}
	public String getIncr_columns() {
		return incr_columns;
	}
	public void setIncr_columns(String incr_columns) {
		this.incr_columns = incr_columns;
	}
	public String getReservior_name() {
		return reservior_name;
	}
	public void setReservior_name(String reservior_name) {
		this.reservior_name = reservior_name;
	}
	
	
	
	
	
}
