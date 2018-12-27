package com.dataextract.dto;

public class ScheduleExtractDto {
	
	String feed_name;
	String sch_flag;
	String cron;
	String file_path;
	String file_pattern;
	String kafka_topic;
	String api_unique_key;
	int project_seq;
	int feed_id;
	
	
	
	
	public String getKafka_topic() {
		return kafka_topic;
	}
	public void setKafka_topic(String kafka_topic) {
		this.kafka_topic = kafka_topic;
	}
	public String getApi_unique_key() {
		return api_unique_key;
	}
	public void setApi_unique_key(String api_unique_key) {
		this.api_unique_key = api_unique_key;
	}
	public int getFeed_id() {
		return feed_id;
	}
	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}
	
	public int getProject_seq() {
		return project_seq;
	}
	public void setProject_seq(int project_seq) {
		this.project_seq = project_seq;
	}
	public String getFeed_name() {
		return feed_name;
	}
	public void setFeed_name(String feed_name) {
		this.feed_name = feed_name;
	}
	public String getSch_flag() {
		return sch_flag;
	}
	public void setSch_flag(String sch_flag) {
		this.sch_flag = sch_flag;
	}
	public String getCron() {
		return cron;
	}
	public void setCron(String cron) {
		this.cron = cron;
	}
	public String getFile_path() {
		return file_path;
	}
	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}
	public String getFile_pattern() {
		return file_pattern;
	}
	public void setFile_pattern(String file_pattern) {
		this.file_pattern = file_pattern;
	}
	
	

}
