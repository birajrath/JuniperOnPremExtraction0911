package com.dataextract.dto;

import java.util.ArrayList;

public class RealTimeExtractDto {
	
	ConnectionDto connDto;
	FeedDto feedDto;
	TableInfoDto tableInfoDto;
	FileInfoDto fileInfoDto;
	HDFSMetadataDto hdfsInfoDto;
	HiveDbMetadataDto hiveInfoDto;
	ArrayList<TargetDto> targetArr;
	
	
	public FileInfoDto getFileInfoDto() {
		return fileInfoDto;
	}
	public void setFileInfoDto(FileInfoDto fileInfoDto) {
		this.fileInfoDto = fileInfoDto;
	}
	public ArrayList<TargetDto> getTargetArr() {
		return targetArr;
	}
	public void setTargetArr(ArrayList<TargetDto> targetArr) {
		this.targetArr = targetArr;
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
	public HDFSMetadataDto getHdfsInfoDto() {
		return hdfsInfoDto;
	}
	public void setHdfsInfoDto(HDFSMetadataDto hdfsInfoDto) {
		this.hdfsInfoDto = hdfsInfoDto;
	}
	public HiveDbMetadataDto getHiveInfoDto() {
		return hiveInfoDto;
	}
	public void setHiveInfoDto(HiveDbMetadataDto hiveInfoDto) {
		this.hiveInfoDto = hiveInfoDto;
	}
}
