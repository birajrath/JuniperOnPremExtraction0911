package com.dataextract.dto;

import java.util.List;

public class HiveDbMetadataDto {
	
	int src_sys_id;
	List<String> hiveDbList;
	String project;
	String juniper_user;
	

	
	
	public List<String> getHiveDbList() {
		return hiveDbList;
	}
	public void setHiveDbList(List<String> hiveDbList) {
		this.hiveDbList = hiveDbList;
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
	public void setProject(String project) {
		this.project = project;
	}
	public String getJuniper_user() {
		return juniper_user;
	}
	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}
	
	
}
