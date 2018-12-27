/**
 * 
 */
package com.dataextract.repositories;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import com.dataextract.constants.OracleConstants;
import com.dataextract.dao.IExtractionDAO;
import com.dataextract.dto.ConnectionDto;
import com.dataextract.dto.FeedDto;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.HiveDbMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.ScheduleExtractDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.util.ConnectionUtils;

/**
 * @author sivakumar.r14
 *
 */
@Component
public class DataExtractRepositoriesImpl implements DataExtractRepositories {


	@Autowired
	IExtractionDAO extractionDao;
	//public IExtractionDAO iExtractionDAO;



	@Override
	public String addConnectionDetails(ConnectionDto dto) throws Exception {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.insertConnectionMetadata(conn,dto);
	}

	@Override
	public String updateConnectionDetails(ConnectionDto connDto) throws SQLException{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.updateConnectionMetadata(conn,connDto);
	}

	@Override
	public String deleteConnectionDetails(ConnectionDto connDto) throws SQLException {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.deleteConnectionMetadata(conn,connDto);
	}

	@Override
	public String addTargetDetails(TargetDto target) throws SQLException {

		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		System.out.println("connection established");
		return extractionDao.insertTargetMetadata(conn,target);
	}

	@Override
	public String updateTargetDetails(TargetDto target) throws SQLException{

		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.updateTargetMetadata(conn,target);
	}


	@Override
	public String onboardSystem(FeedDto feedDto) throws SQLException  {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.insertFeedMetadata(conn, feedDto);

	}

	@Override
	public String updateFeed(FeedDto feedDto)throws SQLException{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.updateFeedMetadata(conn, feedDto);
	}

	@Override
	public String deleteFeed(FeedDto feedDto)throws SQLException {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.deleteFeed(conn, feedDto);
	}


	@Override
	public String addTableDetails(TableInfoDto tableInfoDto) throws SQLException  {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.insertTableMetadata(conn, tableInfoDto);

	}

	@Override
	public String addFileDetails(FileInfoDto fileInfoDto) throws SQLException{

		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.insertFileMetadata(conn, fileInfoDto);
	}

	
	@Override
	public ConnectionDto getConnectionObject(String feed_name) throws SQLException {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getConnectionObject(conn,feed_name);
	}

	@Override
	public FeedDto getFeedObject(String feed_name,String src_type) throws Exception{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getFeedObject(conn,feed_name,src_type);

	}

	@Override
	public ArrayList<TargetDto> getTargetObject(String targetList) throws Exception{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getTargetObject(conn,targetList);
	}

	@Override
	public TableInfoDto getTableInfoObject(String table_list) throws SQLException{

		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getTableInfoObject(conn,table_list);

	}

	@Override
	public FileInfoDto getFileInfoObject(String fileList) throws SQLException{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getFileInfoObject(conn,fileList);
	}
	
	@Override
	public int getProcessGroup(String feed_name, String country_code)throws SQLException{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getProcessGroup(conn,feed_name,country_code);
	}
	
	@Override
	public String checkProcessGroupStatus(int index, String conn_type) throws SQLException{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.checkProcessGroupStatus(conn,index,conn_type);
	}

	@Override
	public String realTimeExtract(RealTimeExtractDto rtExtractDto) throws IOException, SQLException {

		return extractionDao.pullData(rtExtractDto); 
	}

	@Override
	public String batchExtract(ScheduleExtractDto schDto) throws Exception{

		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.createDag(conn,schDto);
	}


	@Override
	public String testConnection(ConnectionDto dto) throws Exception {
		return extractionDao.testConnection(dto);
	}
	
	@Override
	public String testHiveConnection(ConnectionDto dto) throws Exception {
		return extractionDao.testHiveConnection(dto);
	}
	
	@Override
	public String addHiveConnectionDetails(ConnectionDto connDto, String testConnStatus) throws Exception {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		return extractionDao.insertHiveMetadata(conn, connDto,testConnStatus);
	}
	
	

	/* (non-Javadoc)
	 * @see com.dataextract.repositories.DataExtractRepositories#addHDFSDetails(com.dataextract.dto.HDFSMetadataDto)
	 */
	@Override
	public String addHDFSDetails(HDFSMetadataDto hdfsDto) throws SQLException {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		return extractionDao.insertHDFSMetadata(conn, hdfsDto);
	}

	@Override
	public HDFSMetadataDto getHDFSInfoObject(String fileList) throws SQLException {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getHDFSInfoObject(conn,fileList);
	}
	
	@Override
	public String updateNifiProcessgroupDetails(RealTimeExtractDto rtDto,String path,String date,String run_id, int index) throws SQLException{
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.updateNifiProcessgroupDetails(conn, rtDto,path ,date, run_id,index);
	}

	@Override
	public String addHivePropagateDbDetails(HiveDbMetadataDto hivedbDto) throws SQLException {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		return extractionDao.insertHivePropagateMetadata(conn, hivedbDto);
	}
	
	
	@Override
	public HiveDbMetadataDto getHivePropagateInfoObject(String dbList) throws SQLException {
		Connection conn=null;
		conn=ConnectionUtils.connectToOracle(OracleConstants.ORACLE_IP_PORT_SID, OracleConstants.ORACLE_USER_NAME, OracleConstants.ORACLE_PASSWORD);
		//conn= ConnectionUtils.connectToMySql(MySQLConstants.MYSQLIP, MySQLConstants.MYSQLPORT, MySQLConstants.DB,MySQLConstants.USER , MySQLConstants.PASSWORD);
		return extractionDao.getHivePropagateInfoObject(conn,dbList);
	}
}




