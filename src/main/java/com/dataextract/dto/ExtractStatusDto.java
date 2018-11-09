package com.dataextract.dto;

public class ExtractStatusDto {
	
	String src_sys_id;
	String runId;
	String extracted_date;
	String dataPath;
	String metadataPath;
	String status;
	
	
	
	public String getExtracted_date() {
		return extracted_date;
	}
	public void setExtracted_date(String extracted_date) {
		this.extracted_date = extracted_date;
	}
	public String getSrc_sys_id() {
		return src_sys_id;
	}
	public void setSrc_sys_id(String src_sys_id) {
		this.src_sys_id = src_sys_id;
	}
	public String getRunId() {
		return runId;
	}
	public void setRunId(String runId) {
		this.runId = runId;
	}
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	public String getMetadataPath() {
		return metadataPath;
	}
	public void setMetadataPath(String metadataPath) {
		this.metadataPath = metadataPath;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	
	
	

}
