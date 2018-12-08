/**
 * 
 */
package com.dataextract.repositories;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import com.dataextract.dto.ConnectionDto;
import com.dataextract.dto.FeedDto;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.HiveDbMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TargetDto;

/**
 * @author sivakumar.r14
 *
 */
public interface DataExtractRepositories {

public String addConnectionDetails(ConnectionDto dto) throws SQLException, Exception;
public String onboardSystem(FeedDto feedDto) throws SQLException;
public String addTableDetails(TableInfoDto tableInfoDto) throws SQLException;

public ConnectionDto getConnectionObject(String feed_name) throws SQLException;
public FeedDto getFeedObject(String feed_name) throws SQLException;
public TableInfoDto getTableInfoObject(String table_list) throws SQLException;
public String realTimeExtract(RealTimeExtractDto rtExtractDto) throws IOException , SQLException;
public String testConnection(ConnectionDto dto) throws SQLException;
public String addTargetDetails(TargetDto target) throws SQLException;
public ArrayList<TargetDto> getTargetObject(String targetList) throws SQLException;
public String batchExtract(String feed_name,String project,String cron) throws SQLException;
public String updateConnectionDetails(ConnectionDto connDto) throws SQLException;
public String deleteConnectionDetails(ConnectionDto connDto) throws SQLException ;
public String updateFeed(FeedDto feedDto)throws SQLException ;
public String deleteFeed(FeedDto feedDto)throws SQLException ;
public String updateTargetDetails(TargetDto target) throws SQLException;
public String addFileDetails(FileInfoDto fileInfoDto) throws SQLException;
public FileInfoDto getFileInfoObject(String fileList) throws SQLException;
public String addHDFSDetails(HDFSMetadataDto hdfsDto) throws SQLException;
public HDFSMetadataDto getHDFSInfoObject(String fileList)throws SQLException;
public String updateNifiProcessgroupDetails(RealTimeExtractDto rtDto,String path, String date,String run_id,int index) throws SQLException;
public int getProcessGroup(String feed_name, String country_code)throws SQLException;
public String checkProcessGroupStatus(int index, String conn_type) throws SQLException;
public String addHivePropagateDbDetails(HiveDbMetadataDto hivedbDto) throws SQLException;
public HiveDbMetadataDto getHivePropagateInfoObject(String dbList) throws SQLException;







}
