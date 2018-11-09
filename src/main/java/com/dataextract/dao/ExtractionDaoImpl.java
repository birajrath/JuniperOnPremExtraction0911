package com.dataextract.dao;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.dataextract.constants.NifiConstants;
import com.dataextract.constants.OracleConstants;
import com.dataextract.dto.BatchExtractDto;
import com.dataextract.dto.ConnectionDto;
import com.dataextract.dto.FieldMetadataDto;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.FileMetadataDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.SrcSysDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.fetchdata.IExtract;
import com.dataextract.util.ScheduleUtils;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;


@Component
public class ExtractionDaoImpl  implements IExtractionDAO {

	@Autowired
	private IExtract extract;

	@Autowired
	private ScheduleUtils scheduleUtils;

	@Override
	public  int  insertConnectionMetadata(Connection conn, ConnectionDto dto) throws SQLException  {

		String insertQuery="";
		if(dto.getConn_type().equalsIgnoreCase("ORACLE")||dto.getConn_type().equalsIgnoreCase("HADOOP")) 
		{
			insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.CONNECTIONTABLE)
					.replace("{$columns}", "connection_name,connection_type,host_name,port_no,username,password,database_name,service_name,system")
					.replace("{$data}",OracleConstants.QUOTE+dto.getConn_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getHostName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getPort()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getUserName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getPassword()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getDbName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getServiceName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getSystem()+OracleConstants.QUOTE);
		}
		if(dto.getConn_type().equalsIgnoreCase("UNIX")) {

			insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.CONNECTIONTABLE)
					.replace("{$columns}", "connection_name,connection_type,drive_id,path,system")
					.replace("{$data}",OracleConstants.QUOTE+dto.getConn_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
							+dto.getDrive_id()+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getData_path()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getSystem()+OracleConstants.QUOTE);
		}
		if(dto.getConn_type().equalsIgnoreCase("TERADATA")) 
		{
			insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.CONNECTIONTABLE)
					.replace("{$columns}", "connection_name,connection_type,host_name,port_no,username,password,database_name,service_name,system")
					.replace("{$data}",OracleConstants.QUOTE+dto.getConn_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getHostName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getPort()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getUserName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getPassword()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getDbName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getServiceName()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+dto.getSystem()+OracleConstants.QUOTE);
		}

		try {	
			System.out.println(insertQuery);
			Statement statement = conn.createStatement();
			statement.execute(insertQuery);
			return fetchConnectionId(dto.getConn_name(),conn);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			throw e;
		}finally {
			conn.close();
		}




	}


	private int fetchConnectionId(String conn_name, Connection conn) throws SQLException {

		int connectionId=0;
		String query="select connection_id from "+OracleConstants.CONNECTIONTABLE+ " where connection_name='"+conn_name+"'";
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			connectionId=rs.getInt(1);

		}
		return connectionId;

	}


	@Override
	public String updateConnectionMetadata(Connection conn, ConnectionDto connDto) throws SQLException{

		String updateConnectionMaster="";

		if(connDto.getConn_type().equalsIgnoreCase("ORACLE")||connDto.getConn_type().equalsIgnoreCase("HADOOP")) {

			updateConnectionMaster="update "+OracleConstants.CONNECTIONTABLE
					+" set connection_name="+OracleConstants.QUOTE+connDto.getConn_name()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"connection_type="+OracleConstants.QUOTE+connDto.getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"host_name="+OracleConstants.QUOTE+connDto.getHostName()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"port_no="+OracleConstants.QUOTE+connDto.getPort()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"username="+OracleConstants.QUOTE+connDto.getUserName()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"password="+OracleConstants.QUOTE+connDto.getPassword()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"database_name="+OracleConstants.QUOTE+connDto.getDbName()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"service_name="+OracleConstants.QUOTE+connDto.getServiceName()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"system="+OracleConstants.QUOTE+connDto.getSystem()+OracleConstants.QUOTE
					+" where connection_id="+connDto.getConnId();
		}

		if(connDto.getConn_type().equalsIgnoreCase("UNIX")) {

			updateConnectionMaster="update "+OracleConstants.CONNECTIONTABLE
					+" set connection_name="+OracleConstants.QUOTE+connDto.getConn_name()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"connection_type="+OracleConstants.QUOTE+connDto.getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"drive_id="+connDto.getDrive_id()+OracleConstants.COMMA
					+"path="+OracleConstants.QUOTE+connDto.getData_path()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"system="+OracleConstants.QUOTE+connDto.getSystem()+OracleConstants.QUOTE
					+" where connection_id="+connDto.getConnId();
		}

		try {	
			Statement statement = conn.createStatement();
			statement.execute(updateConnectionMaster);
			return "Success";

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();


		}finally {
			conn.close();
		}

	}

	@Override
	public String deleteConnectionMetadata(Connection conn, ConnectionDto connDto)throws SQLException{
		String deleteConnectionMaster="delete from "+OracleConstants.CONNECTIONTABLE+" where connection_id="+connDto.getConnId();
		try {	
			Statement statement = conn.createStatement();
			statement.execute(deleteConnectionMaster);
			return "Success";

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();


		}finally {
			conn.close();
		}

	}


	@Override
	public String insertTargetMetadata(Connection conn, ArrayList<TargetDto> targetArr) throws SQLException {
		String insertTargetDetails="";
		String targetId="";
		StringBuffer targetIds = new StringBuffer();
		String sequence="";
		Statement statement = conn.createStatement();
		for(TargetDto target:targetArr) {
			if(target.getTarget_type().equalsIgnoreCase("gcs")) {
				insertTargetDetails=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TAREGTTABLE)
						.replace("{$columns}", "target_unique_name,target_type,target_project,service_account,target_bucket,system")
						.replace("{$data}", OracleConstants.QUOTE + target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_type()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_project()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getService_account()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_bucket()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getSystem()+OracleConstants.QUOTE
								);
			}
			if(target.getTarget_type().equalsIgnoreCase("hdfs")) {
				insertTargetDetails=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TAREGTTABLE)
						.replace("{$columns}", "target_unique_name,target_type,target_knox_url,target_user,target_password,target_hdfs_path,system")
						.replace("{$data}", OracleConstants.QUOTE + target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_type()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_knox_url()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_user()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_password()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_hdfs_path()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getSystem()+OracleConstants.QUOTE
								);
			}
			 if(target.getTarget_type().equalsIgnoreCase("unix")) {
				insertTargetDetails=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TAREGTTABLE)
						.replace("{$columns}", "target_unique_name,target_type,drive_id,data_path,system")
						.replace("{$data}", OracleConstants.QUOTE + target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_type()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getDrive_id()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getData_path()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getSystem()+OracleConstants.QUOTE
								);
			}

			try {	
				System.out.println("insert statement is "+insertTargetDetails);
				statement.executeUpdate(insertTargetDetails);
				String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.TAREGTTABLE).replace("${columnName}", "TARGET_ID");
				System.out.println("query is "+query);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					System.out.println("sequence is "+sequence);
					//statement.executeQuery("select "+sequence+".nextval from dual");
					rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()) {
						rs.next();
						targetId=rs.getString(1);
						System.out.println("target is"+targetId);
						targetIds.append(targetId+",");
					}
				}
				else {
					System.out.println("empty resultset");
				}


			}catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn.close();
				//TODO: Log the error message
				return "failed due to exception "+e.getMessage();


			}
		}
		targetIds.setLength(targetIds.length()-1);
		conn.close();
		return (targetIds.toString());

	}

	@Override
	public String updateTargetMetadata(Connection conn, ArrayList<TargetDto> targetArr) throws SQLException{


		String updateTargetMaster="";
		for(TargetDto target:targetArr) {
			if(target.getTarget_type().equalsIgnoreCase("gcs")) {
				updateTargetMaster="update "+OracleConstants.TAREGTTABLE 
						+" set target_unique_name="+OracleConstants.QUOTE+target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"target_project="+OracleConstants.QUOTE+target.getTarget_project()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"service_account="+OracleConstants.QUOTE+target.getService_account()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"target_bucket="+OracleConstants.QUOTE+target.getTarget_bucket()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"system="+OracleConstants.QUOTE+target.getSystem()+OracleConstants.QUOTE
						+" where target_id="+target.getTarget_id();
			}
			if(target.getTarget_type().equalsIgnoreCase("hdfs")) { 
				updateTargetMaster="update "+OracleConstants.TAREGTTABLE 
						+" set target_unique_name="+OracleConstants.QUOTE+target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"target_knox_url="+OracleConstants.QUOTE+target.getTarget_knox_url()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"target_user="+OracleConstants.QUOTE+target.getTarget_user()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"target_password="+OracleConstants.QUOTE+target.getTarget_password()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"target_hdfs_path="+OracleConstants.QUOTE+target.getTarget_hdfs_path()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"system="+OracleConstants.QUOTE+target.getSystem()+OracleConstants.QUOTE
						+" where target_id="+target.getTarget_id();
			}
			else if(target.getTarget_type().equalsIgnoreCase("unix")) {
				updateTargetMaster="update "+OracleConstants.TAREGTTABLE 
						+" set target_unique_name="+OracleConstants.QUOTE+target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"drive_id="+OracleConstants.QUOTE+target.getDrive_id()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"data_path="+OracleConstants.QUOTE+target.getData_path()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"system="+OracleConstants.QUOTE+target.getSystem()+OracleConstants.QUOTE
								+" where target_id="+target.getTarget_id();
			}
			try {	
				Statement statement = conn.createStatement();
				statement.execute(updateTargetMaster);


			}catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				//TODO: Log the error message
				return e.getMessage();


			}finally {
				conn.close();
			}


		}
		return "success";

	}


	@Override
	public int insertSrcSysMetadata(Connection conn,SrcSysDto srcSysDto) throws SQLException{

		String insertSourceSystemMaster="";
		String insertExtractionMaster="";
		int src_sys_id=0;
		String sequence="";

		insertSourceSystemMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SOURCESYSTEMTABLE)
				.replace("{$columns}", "src_unique_name,src_sys_desc,country_code,created_by")
				.replace("{$data}", OracleConstants.QUOTE + srcSysDto.getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+srcSysDto.getSrc_sys_desc()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+srcSysDto.getCountry_code()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+srcSysDto.getApplication_user()+OracleConstants.QUOTE
						);

		System.out.println(insertSourceSystemMaster);
		try {	
			Statement statement = conn.createStatement();
			System.out.println("exevuting query");
			statement.execute(insertSourceSystemMaster);
			System.out.println("query executed");
			String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.SOURCESYSTEMTABLE).replace("${columnName}", "SRC_SYS_ID");
			System.out.println("query is "+query);
			ResultSet rs=statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				sequence=rs.getString(1).split("\\.")[1];
				//System.out.println("sequence is "+sequence);
				//statement.executeQuery("select "+sequence+".nextval from dual");
				rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
				if(rs.isBeforeFirst()) {
					rs.next();
					src_sys_id=Integer.parseInt(rs.getString(1));
				}
			}

			System.out.println("source_system id is "+src_sys_id);
			srcSysDto.setSrc_sys_id((src_sys_id));
			if(src_sys_id!=0) {
				insertExtractionMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.EXTRACTIONTABLE)
						.replace("{$columns}","src_sys_id,extraction_type,target,connection_id,created_by,encryptionstatus" )
						.replace("{$data}", src_sys_id +OracleConstants.COMMA
								+OracleConstants.QUOTE+srcSysDto.getSrc_extract_type()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+srcSysDto.getTarget()+OracleConstants.QUOTE+OracleConstants.COMMA
								+srcSysDto.getConnection_id()+OracleConstants.COMMA
								+OracleConstants.QUOTE+srcSysDto.getApplication_user()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+srcSysDto.getEncryptionStatus()+OracleConstants.QUOTE
								);
				Statement statement2=conn.createStatement();
				statement2.execute(insertExtractionMaster);

				return src_sys_id;
			}


		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//TODO: Log the error message
			return 0;


		}finally {
			conn.close();
		}

		return 0;
	}

	@Override
	public String updateSrcSysMetadata(Connection conn, SrcSysDto srcSysDto) throws SQLException{

		String updateSourceSystemMaster="";
		String updateExtractionMaster="";
		updateSourceSystemMaster="update "+OracleConstants.SOURCESYSTEMTABLE 
				+" set src_unique_name="+OracleConstants.QUOTE+srcSysDto.getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
				+"src_sys_desc="+OracleConstants.QUOTE+srcSysDto.getSrc_sys_desc()+OracleConstants.QUOTE+OracleConstants.COMMA
				+"country_code="+OracleConstants.QUOTE+srcSysDto.getCountry_code()+OracleConstants.QUOTE
				+" where src_sys_id="+srcSysDto.getSrc_sys_id();

		updateExtractionMaster="update "+OracleConstants.EXTRACTIONTABLE 
				+" set extraction_type="+OracleConstants.QUOTE+srcSysDto.getSrc_extract_type()+OracleConstants.QUOTE+OracleConstants.COMMA
				+"target="+OracleConstants.QUOTE+srcSysDto.getTarget()+OracleConstants.QUOTE+OracleConstants.COMMA
				+"connection_id="+OracleConstants.QUOTE+srcSysDto.getConnection_id()+OracleConstants.QUOTE+OracleConstants.COMMA
				+"encryptionstatus="+OracleConstants.QUOTE+srcSysDto.getEncryptionStatus()+OracleConstants.QUOTE
				+" where src_sys_id="+srcSysDto.getSrc_sys_id();
		try {	
			Statement statement = conn.createStatement();
			statement.execute(updateSourceSystemMaster);
			statement.execute(updateExtractionMaster);
			return "Success";

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();


		}finally {
			conn.close();
		}
	}

	@Override
	public String deleteSrcSysMetadata(Connection conn, SrcSysDto srcSysDto)throws SQLException{
		String deleteExtactionMaster="delete from "+OracleConstants.EXTRACTIONTABLE+" where src_sys_id="+srcSysDto.getSrc_sys_id();
		String deleteSourceSystemMaster="delete from "+OracleConstants.SOURCESYSTEMTABLE+" where src_sys_id="+srcSysDto.getSrc_sys_id();
		try {	
			Statement statement = conn.createStatement();
			statement.execute(deleteExtactionMaster);
			statement.execute(deleteSourceSystemMaster);
			return "Success";

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();


		}finally {
			conn.close();
		}
	}

	@Override
	public String insertTableMetadata(Connection conn,TableInfoDto tableInfoDto) throws SQLException{

		StringBuffer tableIdList=new StringBuffer();
		ArrayList<Map<String,String>> tableInfo=tableInfoDto.getTableInfo();
		String sequence="";
		for(Map<String,String> table:tableInfo) {
			String insertTableMaster= OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TABLEDETAILSTABLE)
					.replace("{$columns}","src_sys_id,table_name,columns,fetch_type,where_clause,created_by" )
					.replace("{$data}",tableInfoDto.getSrc_sys_id() +OracleConstants.COMMA
							+OracleConstants.QUOTE+table.get("table_name")+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+table.get("columns")+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+table.get("fetch_type")+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+table.get("where_clause")+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+tableInfoDto.getApplication_user()+OracleConstants.QUOTE
							);
			try {	
				Statement statement = conn.createStatement();
				statement.executeUpdate(insertTableMaster);
				String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.TABLEDETAILSTABLE).replace("${columnName}", "TABLE_ID");
				System.out.println("query is "+query);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					//System.out.println("sequence is "+sequence);
					//statement.executeQuery("select "+sequence+".nextval from dual");
					rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()) {
						rs.next();
						tableIdList.append(rs.getString(1)+",");
					}
				}

			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//TODO: Log the error message
				return e.getMessage();


			}

		}
		tableIdList.setLength(tableIdList.length()-1);
		String tableIdListStr=tableIdList.toString();
		String updateExtractionMaster="update "+OracleConstants.EXTRACTIONTABLE+" set table_list='"+tableIdListStr+"' where src_sys_id="+tableInfoDto.getSrc_sys_id();
		try {	
			Statement statement = conn.createStatement();
			statement.execute(updateExtractionMaster);
			return "Success";

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();


		}finally {
			conn.close();
		}

	}


@Override
	public String insertFileMetadata(Connection conn, FileInfoDto fileInfoDto) throws SQLException{
	
		StringBuffer fileList=new StringBuffer();
		
		String insertFileMaster="";
		String sequence="";
		for(FileMetadataDto file:fileInfoDto.getFileMetadataArr()) {
			StringBuffer fieldList=new StringBuffer();
			for(FieldMetadataDto field:fileInfoDto.getFieldMetadataArr()) {
				
				if(field.getFile_name().equalsIgnoreCase(file.getFile_name())) {
					fieldList.append(field.getField_name()+",");
				}
			}
			fieldList.setLength(fieldList.length()-1);
			insertFileMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.FILEDETAILSTABLE)
					.replace("{$columns}","src_sys_id,file_name,file_type,file_delimiter,header_count,trailer_count,field_list,avro_conv_flg" )
					.replace("{$data}",fileInfoDto.getSrc_sys_id() +OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getFile_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getFile_type()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getFile_delimiter()+OracleConstants.QUOTE+OracleConstants.COMMA
							+file.getHeader_count()+OracleConstants.COMMA
							+file.getTrailer_count()+OracleConstants.COMMA
							+OracleConstants.QUOTE+fieldList+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getAvro_conv_flag()+OracleConstants.QUOTE
							);
			try {	
				Statement statement = conn.createStatement();
				System.out.println(insertFileMaster);
				 statement.executeUpdate(insertFileMaster);
				 String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.FILEDETAILSTABLE).replace("${columnName}", "FILE_ID");
				 System.out.println("query is "+query);
				 ResultSet rs=statement.executeQuery(query);
				 if(rs.isBeforeFirst()) {
					 rs.next();
					 sequence=rs.getString(1).split("\\.")[1];
					 //System.out.println("sequence is "+sequence);
					 //statement.executeQuery("select "+sequence+".nextval from dual");
					 rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					 if(rs.isBeforeFirst()) {
						 rs.next();
						 fileList.append(rs.getString(1)+",");
					 }
				 }
				
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//TODO: Log the error message
				return e.getMessage();
				
				
			}
			
			
		}
		fileList.setLength(fileList.length()-1);
		String fileListStr=fileList.toString();
		String updateExtractionMaster="update "+OracleConstants.EXTRACTIONTABLE+" set file_list='"+fileListStr+"' where src_sys_id="+fileInfoDto.getSrc_sys_id();
		try {	
			Statement statement = conn.createStatement();
			statement.execute(updateExtractionMaster);
			return "Success";
			
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();
			
			
		}finally {
			conn.close();
		}
		
	}

	@Override
	public String putFile(Connection conn,FileInfoDto fileInfoDto) throws SQLException, SftpException {

		StringBuffer fileDetails= new StringBuffer();
		StringBuffer fieldDetails=new StringBuffer();
		String data_path=getDataPath(conn,fileInfoDto.getSrc_sys_id());
		if(!data_path.equalsIgnoreCase("failed")) {
			JSch obj_JSch = new JSch();
			try {
				obj_JSch.addIdentity("/home/birajrath2008/.ssh/id_rsa");
				Session obj_Session = null;
				obj_Session = obj_JSch.getSession("birajrath2008", NifiConstants.NIFINSTANCEIP);
				obj_Session.setPort(22);
				// obj_Session.setPassword(str_Password);
				Properties obj_Properties = new Properties();
				obj_Properties.put("StrictHostKeyChecking", "no");
				obj_Session.setConfig(obj_Properties);
				obj_Session.connect();
				Channel obj_Channel = obj_Session.openChannel("sftp");
				obj_Channel.connect();
				ChannelSftp obj_SFTPChannel = (ChannelSftp) obj_Channel;
				fileDetails.append("src_sys_id,file_name,file_type,file_delimiter,header_count,trailer_count,avro_conv_flg");
				fileDetails.append("\n");
				for(FileMetadataDto file:fileInfoDto.getFileMetadataArr()) {
					fileDetails.append(fileInfoDto.getSrc_sys_id()+","+file.getFile_name()+","+file.getFile_type()+","+file.getFile_delimiter()+","+file.getHeader_count()+","+file.getTrailer_count()+","+file.getAvro_conv_flag());
					fileDetails.append("\n");
				}
				fieldDetails.append("src_sys_id,file_id,field_pos,field_name,field_datatype");
				fieldDetails.append("\n");
				for(FieldMetadataDto field:fileInfoDto.getFieldMetadataArr()) {
					fieldDetails.append(fileInfoDto.getSrc_sys_id()+","+field.getFile_name()+","+field.getField_position()+","+field.getField_name()+","+field.getField_datatype());
					fieldDetails.append("\n");
				}
				InputStream fileInputStream = new ByteArrayInputStream(fileDetails.toString().getBytes());
				InputStream fieldInputStream=new ByteArrayInputStream(fieldDetails.toString().getBytes());
				SftpATTRS attrs=null;
				obj_SFTPChannel.cd(data_path);
				try {
					attrs=obj_SFTPChannel.stat("metadata");

				}catch(Exception e) {
					System.out.println("folder does not exists");
					obj_SFTPChannel.mkdir("metadata");
				}

				obj_SFTPChannel.cd("metadata");
				obj_SFTPChannel.put(fileInputStream, data_path+"/metadata/"+"mstr_file_dtls.csv");
				obj_SFTPChannel.put(fieldInputStream, data_path+"/metadata/"+"mstr_field_dtls.csv");
				return "success";

			} catch (JSchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return e.getMessage();
			}
		}
		else {
			return "Error uploading metadata Files";
		}


	}

	private String getDataPath(Connection conn,int src_sys_id) {

		String query1="select connection_id from "+OracleConstants.EXTRACTIONTABLE+" where src_sys_id="+src_sys_id;
		int conn_id;
		int drive_id;
		String path;
		String drive_path;
		try {
			Statement statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(query1);
			if(rs.isBeforeFirst()) {
				rs.next();
				conn_id=rs.getInt(1);
				String query2="select drive_id,path from "+OracleConstants.CONNECTIONTABLE+" where connection_id="+conn_id;
				rs=statement.executeQuery(query2);
				if(rs.isBeforeFirst()) {
					rs.next();
					drive_id=rs.getInt(1);
					path=rs.getString(2);
					String query3="select mounted_path from "+OracleConstants.DRIVETABLE+" where drive_id="+drive_id;
					rs=statement.executeQuery(query3);
					if(rs.isBeforeFirst()) {
						rs.next();
						drive_path=rs.getString(1);
						path=drive_path+path;
						return path;
					}


				}


			}

		}catch(SQLException e){
			e.printStackTrace();
			return "failed";

		}

		return "failed";
	}


	@Override
	public ConnectionDto getConnectionObject(Connection conn,String src_unique_name) throws SQLException {

		int connId=getConnectionId(conn,src_unique_name);
		ConnectionDto connDto=new ConnectionDto();
		String query="select connection_type,host_name,port_no,username,password,database_name,service_name,path from "+OracleConstants.CONNECTIONTABLE+ " where connection_id="+connId;
		try {
			Statement statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				connDto.setConn_type(rs.getString(1));
				connDto.setHostName(rs.getString(2));
				connDto.setPort(rs.getString(3));
				connDto.setUserName(rs.getString(4));
				connDto.setPassword(rs.getString(5));
				connDto.setDbName(rs.getString(6));
				connDto.setServiceName(rs.getString(7));
				connDto.setData_path(rs.getString(8));
			}
			String query2="select drive_id from "+OracleConstants.CONNECTIONTABLE+" where connection_id="+connId;
			rs=statement.executeQuery(query2);
			if(rs.isBeforeFirst()) {
				rs.next();
				connDto.setDrive_id(rs.getInt(1));
			}

		}catch(SQLException e){
			e.printStackTrace();

		}finally {
			conn.close();
		}
		return connDto;

	}

	private int getConnectionId (Connection conn,String src_unique_name) throws SQLException {

		int connectionId=0;
		String query="select e.connection_id from "+OracleConstants.EXTRACTIONTABLE+" e inner join "+OracleConstants.SOURCESYSTEMTABLE+" s on e.src_sys_id=s.src_sys_id where s.src_unique_name='"+src_unique_name+"'";
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			connectionId=rs.getInt(1);

		}
		return connectionId;

	}

	@Override
	public SrcSysDto getSrcSysObject(Connection conn,String src_unique_name) throws SQLException{

		SrcSysDto srcSysDto=new SrcSysDto();

		String query=" select s.src_unique_name,s.country_code,e.target,e.table_list,e.file_list from "+OracleConstants.SOURCESYSTEMTABLE+" s inner join "+OracleConstants.EXTRACTIONTABLE+" e on s.src_sys_id=e.src_sys_id"
				+ " where s.src_unique_name='"+src_unique_name+"'";
		try {
			Statement statement=conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				srcSysDto.setSrc_unique_name(rs.getString(1).toLowerCase());
				srcSysDto.setCountry_code(rs.getString(2).toLowerCase());
				srcSysDto.setTarget(rs.getString(3));
				srcSysDto.setTableList(rs.getString(4));
				srcSysDto.setFileList(rs.getString(5));

			}

			String query2="select src_sys_id from "+OracleConstants.SOURCESYSTEMTABLE+" where src_unique_name='"+src_unique_name+"'";
			rs = statement.executeQuery(query2);
			if(rs.isBeforeFirst()) {
				rs.next();
				srcSysDto.setSrc_sys_id(rs.getInt(1));	
			}

			String data_path=getDataPath(conn, srcSysDto.getSrc_sys_id());
			srcSysDto.setDataPath(data_path);


		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}

		return srcSysDto;

	}

	@Override
	public ArrayList<TargetDto> getTargetObject(Connection conn,String targetList) throws SQLException{

		ArrayList<TargetDto> targetArr=new ArrayList<TargetDto>();
		Statement statement=conn.createStatement();
		String query="";
		ResultSet rs;
		String[] targets=targetList.split(",");;
		try {
			for(String target:targets) {
				TargetDto targetDto=new TargetDto();
				query=" select target_unique_name,target_type,target_project,service_account,target_bucket,target_knox_url,target_user,target_password,target_hdfs_path,drive_id,data_path from "+OracleConstants.TAREGTTABLE
						+ " where target_unique_name='"+target+"'";
				rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					if(rs.getString(2).equalsIgnoreCase("gcs")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						targetDto.setTarget_project(rs.getString(3));
						targetDto.setService_account(rs.getString(4));
						targetDto.setTarget_bucket(rs.getString(5));
					}
					if(rs.getString(2).equalsIgnoreCase("hdfs")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						targetDto.setTarget_knox_url(rs.getString(6));
						targetDto.setTarget_user(rs.getString(7));
						targetDto.setTarget_password(rs.getString(8));
						targetDto.setTarget_hdfs_path(rs.getString(9));
					}
					if(rs.getString(2).equalsIgnoreCase("unix")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						targetDto.setDrive_id(rs.getString(10));
						targetDto.setData_path(rs.getString(11));
						String drivePath=getDrivePath(conn,targetDto.getDrive_id());
						targetDto.setFull_path(drivePath+targetDto.getData_path());
					}



				}
				targetArr.add(targetDto);

			}
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}


		return targetArr;
	}
	
	private String getDrivePath(Connection conn,String driveId) throws SQLException {
		
		String query3="select mounted_path from "+OracleConstants.DRIVETABLE+" where drive_id="+driveId;
		String drivePath="";
		Statement statement=conn.createStatement();
		ResultSet rs=statement.executeQuery(query3);
		if(rs.isBeforeFirst()) {
			rs.next();
			drivePath= rs.getString(1);
		}
		return drivePath;
	}

	@Override
	public TableInfoDto getTableInfoObject(Connection conn,String table_list) throws SQLException{
		TableInfoDto tableInfoDto = new TableInfoDto();
		ArrayList<Map<String,String>> tableInfo=new ArrayList<Map<String,String>>();
		String[] tableIds=table_list.split(",");
		try {
			for(String tableId:tableIds) {
				String query="select table_name,columns,where_clause,fetch_type from "+OracleConstants.TABLEDETAILSTABLE+" where table_id="+tableId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					Map<String,String> tableDetails=new HashMap<String,String>();
					tableDetails.put("table_name", rs.getString(1));
					tableDetails.put("columns",rs.getString(2));
					tableDetails.put("where_clause", rs.getString(3));
					tableDetails.put("fetch_type",  rs.getString(4));
					tableInfo.add(tableDetails);
				}


			}
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}

		tableInfoDto.setTableInfo(tableInfo);
		return tableInfoDto;
	}

	@Override
	public FileInfoDto getFileInfoObject(Connection conn, String fileList) throws SQLException{
		FileInfoDto fileInfoDto= new FileInfoDto();
		ArrayList<FileMetadataDto> fileMetadataArr=new ArrayList<FileMetadataDto>();
		String[] fileIds=fileList.split(",");
		try {
			for(String fileId:fileIds) {
				String query="select file_name,file_type,file_delimiter,header_count,trailer_count,field_list,avro_conv_flg from "+OracleConstants.FILEDETAILSTABLE+" where file_id="+fileId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					FileMetadataDto fileMetadataDto= new FileMetadataDto();
					fileMetadataDto.setFile_name(rs.getString(1));
					fileMetadataDto.setFile_type(rs.getString(2));
					fileMetadataDto.setFile_delimiter(rs.getString(3));
					fileMetadataDto.setHeader_count(rs.getString(4));
					fileMetadataDto.setTrailer_count(rs.getString(5));
					fileMetadataDto.setField_list(rs.getString(6));
					fileMetadataDto.setAvro_conv_flag(rs.getString(7));
					fileMetadataArr.add(fileMetadataDto);
				}
			
				
			}
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}
		
		fileInfoDto.setFileMetadataArr(fileMetadataArr);
		return fileInfoDto;
		
	}

	@Override
	public  String pullData(RealTimeExtractDto rtExtractDto)  {



		String dataPull_status="";
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE")||rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("MSSQL")) {
			String connectionString=getConnectionString(rtExtractDto.getConnDto());
			Long runId=getRunId();
			String date=getDate();
			try {
				dataPull_status= extract.callNifiDataRealTime(rtExtractDto,connectionString,date,runId);
			} catch (UnsupportedOperationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return e.getMessage();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return e.getMessage();
			}

			/*try {
							insertExtractionStatus(conn, rtExtractDto, date, runId);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}*/
			return "Success";
		}
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("UNIX")) {
			Long runId=getRunId();
			String date=getDate();
			try {
				dataPull_status= extract.callNifiUnixRealTime(rtExtractDto,date,runId);
				return "success";
			} catch (UnsupportedOperationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return e.getMessage();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return e.getMessage();
			}
		}
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("HADOOP")) {
			Long runId=getRunId();
			String date=getDate();
			try {
				dataPull_status= extract.callNifiHadoopRealTime(rtExtractDto,date,runId);
				return "success";
			} catch (UnsupportedOperationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return e.getMessage();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return e.getMessage();
			}
			
		}
		
		return "Invalid source";

	}


	private String getDate() {
		Date date = Calendar.getInstance().getTime();

		// Display a date in day, month, year format
		DateFormat formatter = new SimpleDateFormat("ddMMyyyy");
		String today = formatter.format(date);
		return today;
	}


	private Long getRunId() {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Long runId=timestamp.getTime();
		return runId;
	}




	private String getConnectionString(ConnectionDto connDto) {


		if(connDto.getConn_type().equalsIgnoreCase("MSSQL")){

			return "jdbc:sqlserver://"+ connDto.getHostName()+":" +connDto.getPort()+";DatabaseName="+connDto.getDbName() ;
		}
		if(connDto.getConn_type().equalsIgnoreCase("ORACLE"))
		{
			return "jdbc:oracle:thin:@"+ connDto.getHostName()+":" +connDto.getPort()+"/"+connDto.getServiceName() ;
		}
		if(connDto.getConn_type().equalsIgnoreCase("TERADATA"))
		{
			return "jdbc:oracle:thin:@"+ connDto.getHostName()+":" +connDto.getPort()+"/"+connDto.getServiceName() ;
		}

		return null;
	}


	@Override
	public void insertExtractionStatus(Connection conn,RealTimeExtractDto dto,String date,Long runId) throws SQLException {



		String dataPath="gs://"+dto.getSrsSysDto().getTarget()+"/"+dto.getSrsSysDto().getSrc_sys_id()+"-"+dto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/"+"DATA/";
		String metadataPath="gs://"+dto.getSrsSysDto().getTarget()+"/"+dto.getSrsSysDto().getSrc_sys_id()+"-"+dto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/"+"METADATA/";

		// Display a date in day, month, year format
		String insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.EXTRACTSTATUSTABLE)
				.replace("{$columns}", "src_sys_id,extracted_date,run_id,data_path,metadata_path,status")
				.replace("{$data}",OracleConstants.QUOTE+dto.getSrsSysDto().getSrc_sys_id()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+runId+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+dataPath+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+metadataPath+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+"Extracted"+OracleConstants.QUOTE);
		try {	
			Statement statement = conn.createStatement();
			statement.execute(insertQuery);
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			throw e;
		}finally {
			conn.close();
		}


	}
	@Override
	public String createDag(Connection conn,BatchExtractDto batchExtractDto) throws SQLException{

		StringBuffer targetDetails=new StringBuffer();
		for(TargetDto tarDto:batchExtractDto.getTargetArr()) {
			if(tarDto.getTarget_type().equalsIgnoreCase("GCS")) {
				targetDetails.append(tarDto.getTarget_type()+"~");
				targetDetails.append(tarDto.getTarget_project()+"~");
				targetDetails.append(tarDto.getService_account()+"~");
				targetDetails.append(tarDto.getTarget_bucket()+"---");
			}
			if(tarDto.getTarget_type().equalsIgnoreCase("HDFS")) {
				targetDetails.append(tarDto.getTarget_type()+"~");
				targetDetails.append(tarDto.getTarget_knox_url()+"~");
				targetDetails.append(tarDto.getTarget_user()+"~");
				targetDetails.append(tarDto.getTarget_password()+"~");
				targetDetails.append(tarDto.getTarget_hdfs_path()+"---");
			}

		}
		targetDetails.setLength(targetDetails.length()-3);
		String sourceSysDetails=Integer.toString(batchExtractDto.getSrsSysDto().getSrc_sys_id())+"~"+batchExtractDto.getSrsSysDto().getCountry_code()+"~"+batchExtractDto.getSrsSysDto().getSrc_unique_name();
		String connectionString=getConnectionString(batchExtractDto.getConnDto());
		String connectionDetails=connectionString+"~"+batchExtractDto.getConnDto().getConn_type()+"~"+batchExtractDto.getConnDto().getUserName()+"~"+batchExtractDto.getConnDto().getPassword();
		StringBuffer temp=new StringBuffer();
		for(Map<String,String> table: batchExtractDto.getTableInfoDto().getTableInfo()) {

			for (Map.Entry<String,String> entry : table.entrySet()) {
				temp.append(entry.getKey()+"="+entry.getValue()+"/n");
			}
			temp.append("---");
		}
		String tableDetails=temp.toString();

		String status=insertScheduleMetadata(conn,batchExtractDto,connectionDetails,sourceSysDetails,targetDetails.toString(),tableDetails);
		return status;
	}



	private String insertScheduleMetadata(Connection conn,BatchExtractDto batchDto,String connectionDetails,String sourceSysDetails,String targetDetails,String tableDetails) throws SQLException {
		String cron=batchDto.getCron();
		Statement statement=conn.createStatement();
		String insertQuery="";
		String hourlyFlag="";
		String dailyFlag="";
		String monthlyFlag="";
		String weeklyFlag="";
		String[] temp=cron.split(" ");
		String minutes=temp[0];
		String hours=temp[1];
		String dates=temp[2];
		String months=temp[3];
		String daysOfWeek=temp[4];
		if(hours.equals("*")&&dates.equals("*")&&months.equals("*")&&(daysOfWeek.equals("*")) ) {
			hourlyFlag="Y";
		}
		if(dates.equals("*")&&months.equals("*")&&daysOfWeek.equals("*")&&!hours.equals("*")&&!minutes.equals("*")) {
			System.out.println("this is a dailyBatch");
			dailyFlag="Y";
		}
		if(months.equals("*")&&daysOfWeek.equals("*")&&!dates.equals("*")&&!hours.equals("*")&&!minutes.equals("*")) {
			System.out.println("this is a monthlyBatch");
			monthlyFlag="Y";
		}
		if(dates.equals("*")&&months.equals("*")&&!minutes.equals("*")&&!hours.equals("*")&&!daysOfWeek.equals("*")) {
			weeklyFlag="Y";
			System.out.println("this is weeklyBatch");
		}



		try {
			if(dailyFlag.equalsIgnoreCase("Y")) {
				if(hours.contains(",")) {
					for(String hour:hours.split(",")) {
						if(minutes.contains(",")) {
							for(String minute:minutes.split(",")) {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
										.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA

												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE);
								System.out.println(insertQuery);
								statement.execute(insertQuery);


							}
						}else {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
									.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA

											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE);
							System.out.println(insertQuery);
							statement.execute(insertQuery);
						}
					}
				}else {
					if(minutes.contains(",")) {
						for(String minute:minutes.split(",")) {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
									.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA

											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE);
							System.out.println(insertQuery);
							statement.execute(insertQuery);


						}
					}else {
						insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
								.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
								.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA

										+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE);
						System.out.println(insertQuery);
						statement.execute(insertQuery);
					}
				}
			} if(monthlyFlag.equalsIgnoreCase("Y")) {
				if(dates.contains(",")) {
					for(String date:dates.split(",")) {
						if(hours.contains(",")) {
							for(String hour:hours.split(",")) {
								if(minutes.contains(",")) {
									for(String minute:minutes.split(",")) {
										insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
												.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
												.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE);
										System.out.println(insertQuery);
										statement.execute(insertQuery); 
									}

								}
								else {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
											.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE);
									System.out.println(insertQuery);
									statement.execute(insertQuery);
								}
							}




						}else {
							if(minutes.contains(",")) {
								for(String minute:minutes.split(",")) {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
											.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE);
									System.out.println(insertQuery);
									statement.execute(insertQuery); 
								}

							} 			else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
										.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE);
								System.out.println(insertQuery);
								statement.execute(insertQuery); 
							}	
						}
					}
				}
				else {
					if(hours.contains(",")) {
						for(String hour:hours.split(",")) {
							if(minutes.contains(",")) {
								for(String minute:minutes.split(",")) {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
											.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE);
									System.out.println(insertQuery);
									statement.execute(insertQuery); 
								}

							}
							else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
										.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE);
								System.out.println(insertQuery);
								statement.execute(insertQuery);
							}
						}




					}else {
						if(minutes.contains(",")) {
							for(String minute:minutes.split(",")) {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
										.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE);
								System.out.println(insertQuery);
								statement.execute(insertQuery); 
							}

						}else {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,daily_flag,job_schedule_time")
									.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE);
							System.out.println(insertQuery);
							statement.execute(insertQuery);
						}	
					}
				}
			}if(weeklyFlag.equalsIgnoreCase("Y")) {
				if(daysOfWeek.contains(",")) {
					for(String day:daysOfWeek.split(",")) {
						if(hours.contains(",")) {
							for(String hour:hours.split(",")) {
								if(minutes.contains(",")) {
									for(String minute:minutes.split(",")) {
										insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
												.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,weekly_flag,week_run_day,job_schedule_time")
												.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE);
										statement.execute(insertQuery);
									}
								}else {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,weekly_flag,week_run_day,job_schedule_time")
											.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weekExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE);
									statement.execute(insertQuery);
								}
							}
						}else {

							if(minutes.contains(",")) {
								for(String minute:minutes.split(",")) {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,weekly_flag,week_run_day,job_schedule_time")
											.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE);
									statement.execute(insertQuery);
								}
							}else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE);
								statement.execute(insertQuery);
							}
						}
					}
				}else {
					if(hours.contains(",")) {
						for(String hour:hours.split(",")) {
							if(minutes.contains(",")) {
								for(String minute:minutes.split(",")) {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE);
									statement.execute(insertQuery);
								}
							}else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,weekly_flag,week_run_day,job_schedule_time")
										.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE);
								statement.execute(insertQuery);
							}
						}
					}else {

						if(minutes.contains(",")) {
							for(String minute:minutes.split(",")) {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,weekly_flag,week_run_day,job_schedule_time")
										.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE);
								statement.execute(insertQuery);
							}
						}else {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,argument_2,argument_3,argument_4,weekly_flag,week_run_day,job_schedule_time")
									.replace("{$data}",OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+"_Extraction"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+batchDto.getSrsSysDto().getSrc_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"/home/juniper/scripts/pullData.sh"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+connectionDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+sourceSysDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+targetDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+tableDetails+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE);
							statement.execute(insertQuery);
						}
					}
				}
			}
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}





		return "success";
	}


	@Override
	public String insertHDFSMetadata(Connection conn, HDFSMetadataDto hdfsDto) throws SQLException {

		StringBuffer fileList=new StringBuffer();
		String insertHDFSMaster="";
		String sequence="";
		for(String filePath:hdfsDto.getHdfsPath()) {
			
			insertHDFSMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.HDFSDETAILSTABLE)
					.replace("{$columns}","src_sys_id,hdfs_path" )
					.replace("{$data}",hdfsDto.getSrc_sys_id() +OracleConstants.COMMA
							+OracleConstants.QUOTE+filePath+OracleConstants.QUOTE);
			try {	
				Statement statement = conn.createStatement();
				System.out.println(insertHDFSMaster);
				statement.executeUpdate(insertHDFSMaster);
				String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.HDFSDETAILSTABLE).replace("${columnName}", "HDFS_ID");
				System.out.println("query is "+query);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					//System.out.println("sequence is "+sequence);
					//statement.executeQuery("select "+sequence+".nextval from dual");
					rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()) {
						rs.next();
						fileList.append(rs.getString(1)+",");
					}
				}

			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//TODO: Log the error message
				return e.getMessage();
			}

		}
		
		fileList.setLength(fileList.length()-1);
		String fileListStr=fileList.toString();
		String updateExtractionMaster="update "+OracleConstants.EXTRACTIONTABLE+" set file_list='"+fileListStr+"' where src_sys_id="+hdfsDto.getSrc_sys_id();
		try {	
			Statement statement = conn.createStatement();
			statement.execute(updateExtractionMaster);
			return "Success";

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			return e.getMessage();


		}finally {
			conn.close();
		}

	}


	@Override
	public HDFSMetadataDto getHDFSInfoObject(Connection conn, String fileList) throws SQLException {
		
		HDFSMetadataDto hdfsInfoDto= new HDFSMetadataDto();
		ArrayList<String> filePaths=new ArrayList<String>();
		String[] fileIds=fileList.split(",");
		try {
			for(String fileId:fileIds) {
				String query="select hdfs_path from "+OracleConstants.HDFSDETAILSTABLE+" where hdfs_id="+fileId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					filePaths.add(rs.getString(1));
				}
			}
			hdfsInfoDto.setHdfsPath(filePaths);
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}

return hdfsInfoDto;
	}
}








