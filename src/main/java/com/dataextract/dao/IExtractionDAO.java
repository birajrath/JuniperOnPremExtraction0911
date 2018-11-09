package com.dataextract.dao;

import java.sql.Connection;

import java.sql.SQLException;
import java.util.ArrayList;

import com.dataextract.dto.DataExtractDto;
import com.dataextract.dto.ExtractStatusDto;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.FileMetadataDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.SrcSysDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TargetDto;
import com.jcraft.jsch.SftpException;
import com.dataextract.dto.BatchExtractDto;
import com.dataextract.dto.ConnectionDto;

public interface IExtractionDAO {
	
	
	public int insertConnectionMetadata(Connection conn,ConnectionDto dto) throws SQLException;
	public int insertSrcSysMetadata(Connection conn,SrcSysDto srcSysDto) throws SQLException;
	public String insertTableMetadata(Connection conn,TableInfoDto tableInfoDto) throws SQLException;
	public ConnectionDto getConnectionObject(Connection conn,String src_unique_name) throws SQLException;
	public SrcSysDto getSrcSysObject(Connection conn,String src_unique_name) throws SQLException;
	public TableInfoDto getTableInfoObject(Connection conn,String table_list) throws SQLException;
	public  String pullData(RealTimeExtractDto rtExtractDto);
	public void insertExtractionStatus(Connection conn,RealTimeExtractDto dto,String date,Long runId) throws SQLException;
	public String insertTargetMetadata(Connection conn, ArrayList<TargetDto> targetArr) throws SQLException;
	public ArrayList<TargetDto> getTargetObject(Connection conn,String targetList) throws SQLException;
	public String createDag(Connection conn,BatchExtractDto batchExtractDto) throws SQLException;
	public String updateConnectionMetadata(Connection conn, ConnectionDto connDto) throws SQLException;
	public String deleteConnectionMetadata(Connection conn, ConnectionDto connDto)throws SQLException;
	public String updateSrcSysMetadata(Connection conn, SrcSysDto srcSysDto) throws SQLException;
	public String deleteSrcSysMetadata(Connection conn, SrcSysDto srcSysDto)throws SQLException;
	public String updateTargetMetadata(Connection conn, ArrayList<TargetDto> targetArr) throws SQLException;
	public String insertFileMetadata(Connection conn, FileInfoDto fileInfoDto) throws SQLException;
	public String putFile(Connection conn,FileInfoDto fileInfoDto) throws SQLException, SftpException;
	public FileInfoDto getFileInfoObject(Connection conn, String fileList) throws SQLException;
	public String insertHDFSMetadata(Connection conn, HDFSMetadataDto hdfsDto)throws SQLException;
	public HDFSMetadataDto getHDFSInfoObject(Connection conn, String fileList)throws SQLException;
	
	
	
	
	
	
	
}
