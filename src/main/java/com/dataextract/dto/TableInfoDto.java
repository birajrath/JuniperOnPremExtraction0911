package com.dataextract.dto;

import java.util.ArrayList;
import java.util.Map;

public class TableInfoDto {
	
	String application_user;
	int src_sys_id;
	ArrayList<Map<String,String>> tableInfo;
	
	
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
	public ArrayList<Map<String, String>> getTableInfo() {
		return tableInfo;
	}
	public void setTableInfo(ArrayList<Map<String, String>> tableInfo) {
		this.tableInfo = tableInfo;
	}
	

}
