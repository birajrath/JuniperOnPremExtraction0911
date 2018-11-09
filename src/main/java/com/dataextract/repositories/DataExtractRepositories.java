/**
 * 
 */
package com.dataextract.repositories;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


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

/**
 * @author sivakumar.r14
 *
 */
public interface DataExtractRepositories {

public int addConnectionDetails(ConnectionDto dto) throws SQLException;
public int onboardSystem(SrcSysDto srcSysDto) throws SQLException;
public String addTableDetails(TableInfoDto tableInfoDto) throws SQLException;

public ConnectionDto getConnectionObject(String src_unique_name) throws SQLException;
public SrcSysDto getSrcSysObject(String src_unique_name) throws SQLException;
public TableInfoDto getTableInfoObject(String table_list) throws SQLException;
public String realTimeExtract(RealTimeExtractDto rtExtractDto) throws IOException , SQLException;
public String testConnection(ConnectionDto dto);
public String addTargetDetails(ArrayList<TargetDto> targetArr) throws SQLException;
public ArrayList<TargetDto> getTargetObject(String targetList) throws SQLException;
public String batchExtract(BatchExtractDto batchExtractDto) throws SQLException;
public String updateConnectionDetails(ConnectionDto connDto) throws SQLException;
public String deleteConnectionDetails(ConnectionDto connDto) throws SQLException ;
public String updateSystem(SrcSysDto srcSysDto)throws SQLException ;
public String deleteSystem(SrcSysDto srcSysDto)throws SQLException ;
public String updateTargetDetails(ArrayList<TargetDto> targetArr) throws SQLException;
public String addFileDetails(FileInfoDto fileInfoDto) throws SQLException;
public String putFile(FileInfoDto fileInfoDto) throws SQLException, SftpException;
public FileInfoDto getFileInfoObject(String fileList) throws SQLException;
public String addHDFSDetails(HDFSMetadataDto hdfsDto) throws SQLException;
public HDFSMetadataDto getHDFSInfoObject(String fileList)throws SQLException;;






}
