package com.dataextract.dto;

import java.util.ArrayList;

public class HDFSMetadataDto {
	
	int src_sys_id;
	ArrayList<String> hdfsPath;
	

	
	public ArrayList<String> getHdfsPath() {
		return hdfsPath;
	}
	public void setHdfsPath(ArrayList<String> hdfsPath) {
		this.hdfsPath = hdfsPath;
	}
	public int getSrc_sys_id() {
		return src_sys_id;
	}
	public void setSrc_sys_id(int src_sys_id) {
		this.src_sys_id = src_sys_id;
	}
}
