package com.dataextract.dto;

import java.util.List;

public class HiveDbMetadataDto {
	
	int feed_id;
	List<String> hiveDbList;
	String project;
	String juniper_user;
	

	
	
	public int getFeed_id() {
		return feed_id;
	}
	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}
	public List<String> getHiveDbList() {
		return hiveDbList;
	}
	public void setHiveDbList(List<String> hiveDbList) {
		this.hiveDbList = hiveDbList;
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
