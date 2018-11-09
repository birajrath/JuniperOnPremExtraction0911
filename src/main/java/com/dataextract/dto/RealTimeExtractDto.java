package com.dataextract.dto;

import java.util.ArrayList;

public class RealTimeExtractDto {
	
	ConnectionDto connDto;
	SrcSysDto srsSysDto;
	TableInfoDto tableInfoDto;
	FileInfoDto fileInfoDto;
	//HDFSMetadataDto hdfsInfoDto;
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
	public SrcSysDto getSrsSysDto() {
		return srsSysDto;
	}
	public void setSrsSysDto(SrcSysDto srsSysDto) {
		this.srsSysDto = srsSysDto;
	}
	public TableInfoDto getTableInfoDto() {
		return tableInfoDto;
	}
	public void setTableInfoDto(TableInfoDto tableInfoDto) {
		this.tableInfoDto = tableInfoDto;
	}
	/*public HDFSMetadataDto getHdfsInfoDto() {
		return hdfsInfoDto;
	}
	public void setHdfsInfoDto(HDFSMetadataDto hdfsInfoDto) {
		this.hdfsInfoDto = hdfsInfoDto;
	}*/
	
	

}
