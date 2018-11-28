package com.dataextract.dao;

import java.sql.Connection;

import java.sql.SQLException;
import java.util.ArrayList;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.FeedDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.dto.BatchExtractDto;
import com.dataextract.dto.ConnectionDto;

public interface IExtractionDAO {
	
	
	public String insertConnectionMetadata(Connection conn,ConnectionDto dto) throws SQLException, Exception;
	public String insertFeedMetadata(Connection conn,FeedDto srcSysDto) throws SQLException;
	public String insertTableMetadata(Connection conn,TableInfoDto tableInfoDto) throws SQLException;
	public ConnectionDto getConnectionObject(Connection conn,String feed_name) throws SQLException;
	public FeedDto getFeedObject(Connection conn,String src_unique_name) throws SQLException;
	public TableInfoDto getTableInfoObject(Connection conn,String table_list) throws SQLException;
	public  String pullData(RealTimeExtractDto rtExtractDto);
	public String insertTargetMetadata(Connection conn, TargetDto target) throws SQLException;
	public ArrayList<TargetDto> getTargetObject(Connection conn,String targetList) throws SQLException;
	public String createDag(Connection conn,BatchExtractDto batchExtractDto) throws SQLException;
	public String updateConnectionMetadata(Connection conn, ConnectionDto connDto) throws SQLException;
	public String deleteConnectionMetadata(Connection conn, ConnectionDto connDto)throws SQLException;
	public String updateFeedMetadata(Connection conn, FeedDto feedDto) throws SQLException;
	public String deleteFeed(Connection conn, FeedDto srcSysDto)throws SQLException;
	public String updateTargetMetadata(Connection conn, TargetDto target) throws SQLException;
	public String insertFileMetadata(Connection conn, FileInfoDto fileInfoDto) throws SQLException;
	public FileInfoDto getFileInfoObject(Connection conn, String fileList) throws SQLException;
	public String insertHDFSMetadata(Connection conn, HDFSMetadataDto hdfsDto)throws SQLException;
	public HDFSMetadataDto getHDFSInfoObject(Connection conn, String fileList)throws SQLException;
	public String updateNifiProcessgroupDetails(Connection conn, RealTimeExtractDto rtDto,String path,String date, String run_id, int index) throws SQLException;
	public int getProcessGroup(Connection conn, String feed_name, String country_code) throws SQLException;
	public String checkProcessGroupStatus(Connection conn, int index, String conn_type) throws SQLException;
	
	
	
	
	
	
	
	
}
