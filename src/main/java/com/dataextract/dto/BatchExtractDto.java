package com.dataextract.dto;

import java.util.ArrayList;

public class BatchExtractDto {

	
	ConnectionDto connDto;
	FeedDto feedDto;
	TableInfoDto tableInfoDto;
	FileInfoDto fileInfoDto;
	HDFSMetadataDto hdfsInfoDto;
	ArrayList<TargetDto> targetArr=new ArrayList<TargetDto>();
	String cron;
	
	
	
	public FileInfoDto getFileInfoDto() {
		return fileInfoDto;
	}
	public void setFileInfoDto(FileInfoDto fileInfoDto) {
		this.fileInfoDto = fileInfoDto;
	}
	public HDFSMetadataDto getHdfsInfoDto() {
		return hdfsInfoDto;
	}
	public void setHdfsInfoDto(HDFSMetadataDto hdfsInfoDto) {
		this.hdfsInfoDto = hdfsInfoDto;
	}
	public ArrayList<TargetDto> getTargetArr() {
		return targetArr;
	}
	public void setTargetArr(ArrayList<TargetDto> targetArr) {
		this.targetArr = targetArr;
	}
	
	public String getCron() {
		return cron;
	}
	public void setCron(String cron) {
		this.cron = cron;
	}
	public ConnectionDto getConnDto() {
		return connDto;
	}
	public void setConnDto(ConnectionDto connDto) {
		this.connDto = connDto;
	}
	
	public FeedDto getFeedDto() {
		return feedDto;
	}
	public void setFeedDto(FeedDto feedDto) {
		this.feedDto = feedDto;
	}
	public TableInfoDto getTableInfoDto() {
		return tableInfoDto;
	}
	public void setTableInfoDto(TableInfoDto tableInfoDto) {
		this.tableInfoDto = tableInfoDto;
	}
	
}
