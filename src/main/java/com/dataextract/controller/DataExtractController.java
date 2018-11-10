/**
 * 
 */
package com.dataextract.controller;

import java.sql.SQLException;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


import com.dataextract.dto.DataExtractDto;
import com.dataextract.dto.ExtractStatusDto;
import com.dataextract.dto.ExtracteRequestDto;
import com.dataextract.dto.FieldMetadataDto;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.FileMetadataDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.NifiRequestDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.RequestDto;
import com.dataextract.dto.SrcSysDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.dto.UnixDataRequestDto;
import com.dataextract.constants.GenericConstants;
import com.dataextract.dto.BatchExtractDto;
import com.dataextract.dto.ConnectionDto;
import com.dataextract.repositories.DataExtractRepositories;
import com.dataextract.util.ResponseUtil;
import com.jcraft.jsch.SftpException;

/**
 * @author sivakumar.r14
 *
 */
@CrossOrigin
@RestController
public class DataExtractController {

	@Autowired
	private DataExtractRepositories dataExtractRepositories;

	
	

	@RequestMapping(value = "/addConnection", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addConnection(@RequestBody RequestDto requestDto) {
		// Parse json to Dto Object
		String testConnStatus="";
		String status = "";
		String message = "";
		ConnectionDto connDto = new ConnectionDto();
		int connectionId=0;
		
		if(requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("ORACLE")
				||requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("HADOOP")) {
			System.out.println("oracle system or hadoop system");
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
			connDto.setHostName(requestDto.getBody().get("data").get("host_name"));
			connDto.setPort(requestDto.getBody().get("data").get("port"));
			connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
			connDto.setPassword(requestDto.getBody().get("data").get("password"));
			connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
			connDto.setServiceName(requestDto.getBody().get("data").get("service_name"));
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
		}
		if(requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("UNIX")) {
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
			connDto.setDrive_id(Integer.parseInt(requestDto.getBody().get("data").get("drive_id")));
			connDto.setData_path(requestDto.getBody().get("data").get("data_path"));
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
		}
		
		if(requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("TERADATA")) {
			System.out.println("Teradata System");
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
			connDto.setHostName("144.21.70.142");
			connDto.setPort("1521");
			connDto.setUserName("arg_loans_db");
			connDto.setPassword("cdc1");
			connDto.setServiceName("PDB1.611065800.oraclecloud.internal");
			connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
		}
		
		try {
			testConnStatus=dataExtractRepositories.testConnection(connDto);
			if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
				connectionId = dataExtractRepositories.addConnectionDetails(connDto);
				status="success";
			}
			else {
				status="Failed";
				message=testConnStatus;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status = "Failed";
		}

		if (status.equalsIgnoreCase("Success")) {
			
			
			message = "Connection tested and thed details have been saved Successfully. The connection id is "+connectionId;
		} else {

			message = "Error While Adding connection details";
		}
		return ResponseUtil.createResponse(status, message);
	}
	
	
	@RequestMapping(value = "/updConnection", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updConnection(@RequestBody RequestDto requestDto) {
		// Parse json to Dto Object
		String testConnStatus="";
		String status = "";
		String message = "";
		ConnectionDto connDto = new ConnectionDto();
		
		
		connDto.setConnId(Integer.parseInt(requestDto.getBody().get("data").get("conn")));
		connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
		connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
		connDto.setSystem(requestDto.getBody().get("data").get("system"));
		
		if(connDto.getConn_type().equalsIgnoreCase("ORACLE")||connDto.getConn_type().equalsIgnoreCase("HADOOP")) {
			
			connDto.setHostName(requestDto.getBody().get("data").get("host_name"));
			connDto.setPort(requestDto.getBody().get("data").get("port"));
			connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
			connDto.setPassword(requestDto.getBody().get("data").get("password"));
			connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
			connDto.setServiceName(requestDto.getBody().get("data").get("service_name"));
		}
		if(connDto.getConn_type().equalsIgnoreCase("UNIX")){
			
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setDrive_id(Integer.parseInt(requestDto.getBody().get("data").get("drive_id")));
			connDto.setData_path(requestDto.getBody().get("data").get("data_path"));
		}
		
		try {
			testConnStatus=dataExtractRepositories.testConnection(connDto);
			if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
				status = dataExtractRepositories.updateConnectionDetails(connDto);
			}
			else {
				status="Failed";
				message=testConnStatus;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status = "Failed";
		}

		if (status.equalsIgnoreCase("Success")) {
			
			
			message = "Connection updated Successfully";
		} else {

			message = "Error While updating connection details";
		}
		return ResponseUtil.createResponse(status, message);
	}
	
	
	
	@RequestMapping(value = "/delConnection", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String delConnection(@RequestBody RequestDto requestDto) {
		// Parse json to Dto Object
		
		String status = "";
		String message = "";
		String response="";
		ConnectionDto connDto = new ConnectionDto();
		
		
		connDto.setConnId(Integer.parseInt(requestDto.getBody().get("data").get("conn")));
	
		try {
				response = dataExtractRepositories.deleteConnectionDetails(connDto);
			}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status = "Failed";
			message=response;
		}

		if (response.equalsIgnoreCase("Success")) {
			
			status="success";
			message = "Connection updated Successfully";
		} 
		return ResponseUtil.createResponse(status, message);
	}
	
	
	
	@RequestMapping(value = "/addTarget", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addTarget(@RequestBody RequestDto requestDto) throws SQLException {
		// Parse json to Dto Object
		String targetIds="";
		String status = "";
		String message = "";
		
		ArrayList<TargetDto> targetArr=new ArrayList<TargetDto>();
		int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
		
		
		for(int i=1;i<=counter;i++ ) {
			TargetDto target=new TargetDto();
			if(requestDto.getBody().get("data").get("target_type"+i).equalsIgnoreCase("gcs")){
				target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"+i));
				target.setTarget_type(requestDto.getBody().get("data").get("target_type"+i));
				target.setTarget_project(requestDto.getBody().get("data").get("target_project"+i));
				target.setService_account(requestDto.getBody().get("data").get("service_account"+i));
				target.setTarget_bucket(requestDto.getBody().get("data").get("target_bucket"+i));
				target.setSystem(requestDto.getBody().get("data").get("system"));
			}
			if(requestDto.getBody().get("data").get("target_type"+i).equalsIgnoreCase("hdfs")){
				target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"+i));
				target.setTarget_type(requestDto.getBody().get("data").get("target_type"+i));
				target.setTarget_knox_url(requestDto.getBody().get("data").get("knox_url"+i));
				target.setTarget_user(requestDto.getBody().get("data").get("username"+i));
				target.setTarget_password(requestDto.getBody().get("data").get("password"+i));
				target.setTarget_hdfs_path(requestDto.getBody().get("data").get("hadoop_path"+i));
				target.setSystem(requestDto.getBody().get("data").get("system"));
			}
			if(requestDto.getBody().get("data").get("target_type"+i).equalsIgnoreCase("unix")){
				
				System.out.println("unix target");
				target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"+i));
				target.setTarget_type(requestDto.getBody().get("data").get("target_type"+i));
				target.setDrive_id(requestDto.getBody().get("data").get("drive_id"+i));
				target.setData_path(requestDto.getBody().get("data").get("data_path"+i));
	
				target.setSystem(requestDto.getBody().get("data").get("system"));
			}
			
			targetArr.add(target);
		}
		
		

			targetIds = dataExtractRepositories.addTargetDetails(targetArr);
			if(targetIds.contains("failed")) {
				status="failed";
				message=targetIds;
				
			}
			else {
				status="success";
				message="Target details added succesfully. The target unique ids are "+targetIds;
			}
		return ResponseUtil.createResponse(status, message);
	}
	
	
	@RequestMapping(value = "/updTarget", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updTarget(@RequestBody RequestDto requestDto) throws SQLException {
		// Parse json to Dto Object
		String response="";
		String status = "";
		String message = "";
		
		ArrayList<TargetDto> targetArr=new ArrayList<TargetDto>();
		int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
		
		for(int i=1;i<=counter;i++ ) {
			TargetDto target=new TargetDto();
			if(requestDto.getBody().get("data").get("target_type"+i).equalsIgnoreCase("gcs")){
				target.setTarget_id(Integer.parseInt(requestDto.getBody().get("data").get("tgt"+i)));
				target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"+i));
				target.setTarget_type(requestDto.getBody().get("data").get("target_type"+i));
				target.setTarget_project(requestDto.getBody().get("data").get("target_project"+i));
				target.setService_account(requestDto.getBody().get("data").get("service_account"+i));
				target.setTarget_bucket(requestDto.getBody().get("data").get("target_bucket"+i));
				target.setSystem(requestDto.getBody().get("data").get("system"+i));
			}
			if(requestDto.getBody().get("data").get("target_type"+i).equalsIgnoreCase("hdfs")){
				target.setTarget_id(Integer.parseInt(requestDto.getBody().get("data").get("tgt"+i)));
				target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"+i));
				target.setTarget_type(requestDto.getBody().get("data").get("target_type"+i));
				target.setTarget_knox_url(requestDto.getBody().get("data").get("knox_url"+i));
				target.setTarget_user(requestDto.getBody().get("data").get("username"+i));
				target.setTarget_password(requestDto.getBody().get("data").get("password"+i));
				target.setTarget_hdfs_path(requestDto.getBody().get("data").get("hadoop_path"+i));
				target.setSystem(requestDto.getBody().get("data").get("system"+i));
			}
			if(requestDto.getBody().get("data").get("target_type"+i).equalsIgnoreCase("unix")){
				target.setTarget_id(Integer.parseInt(requestDto.getBody().get("data").get("tgt"+i)));
				target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"+i));
				target.setTarget_type(requestDto.getBody().get("data").get("target_type"+i));
				target.setDrive_id(requestDto.getBody().get("data").get("drive_id"+i));
				target.setData_path(requestDto.getBody().get("data").get("data_path"+i));
				target.setSystem(requestDto.getBody().get("data").get("system"+i));
			}
			
			targetArr.add(target);
		}
		
		

			response = dataExtractRepositories.updateTargetDetails(targetArr);
			if(response.equalsIgnoreCase("success")) {
				status="success";
				message="Target Details added successfully";
				
			}
			else {
				status="failed";
				message=response;
			}
		return ResponseUtil.createResponse(status, message);
	}
	
	
	
	
	@RequestMapping(value = "/onboardSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String onBoardSystem(@RequestBody RequestDto requestDto) {
		String response="";
		String status="";
		String message="";
		int src_sys_id=0;
		SrcSysDto srcSysDto = new SrcSysDto();
		srcSysDto.setApplication_user(requestDto.getHeader().get("user"));
		srcSysDto.setSrc_unique_name(requestDto.getBody().get("data").get("src_unique_name"));
		srcSysDto.setConnection_id(Integer.parseInt(requestDto.getBody().get("data").get("connection_id")));
		srcSysDto.setSrc_sys_desc(requestDto.getBody().get("data").get("src_sys_desc"));
		srcSysDto.setCountry_code(requestDto.getBody().get("data").get("country_code"));
		srcSysDto.setSrc_extract_type(requestDto.getBody().get("data").get("src_extract_type"));
		srcSysDto.setTarget(requestDto.getBody().get("data").get("target"));
		srcSysDto.setEncryptionStatus(requestDto.getBody().get("data").get("encrypt"));
		try {
			 src_sys_id=dataExtractRepositories.onboardSystem(srcSysDto);
			 
			 if(src_sys_id!=0) {
				 response="Success";
			 }
			
			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Source System Onboared. The Source System Id is "+srcSysDto.getSrc_sys_id();
			}
			else {
				status="Failed";
				message="Error while onboarding system";
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status="Failed";
			message=e.getMessage();
		}
		
		// Parse Json to Dto Object
		return ResponseUtil.createResponse(status, message);
	}
	
	@RequestMapping(value = "/updSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updSystem(@RequestBody RequestDto requestDto) {
		String response="";
		String status="";
		String message="";
		SrcSysDto srcSysDto = new SrcSysDto();
		srcSysDto.setApplication_user(requestDto.getHeader().get("user"));
		srcSysDto.setSrc_unique_name(requestDto.getBody().get("data").get("src_unique_name"));
		srcSysDto.setConnection_id(Integer.parseInt(requestDto.getBody().get("data").get("connection_id")));
		srcSysDto.setSrc_sys_desc(requestDto.getBody().get("data").get("src_sys_desc"));
		srcSysDto.setCountry_code(requestDto.getBody().get("data").get("country_code"));
		srcSysDto.setSrc_extract_type(requestDto.getBody().get("data").get("src_extract_type"));
		srcSysDto.setTarget(requestDto.getBody().get("data").get("target"));
		srcSysDto.setSrc_sys_id(Integer.parseInt(requestDto.getBody().get("data").get("src_sys")));
		try {
			 response=dataExtractRepositories.updateSystem(srcSysDto);
			 
			
			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Source System updated";
			}
			else {
				status="Failed";
				message=response;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status="Failed";
			message=e.getMessage();
		}
		
		// Parse Json to Dto Object
		return ResponseUtil.createResponse(status, message);
	}
	
	@RequestMapping(value = "/delSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String delSystem(@RequestBody RequestDto requestDto) {
		String response="";
		String status="";
		String message="";
		SrcSysDto srcSysDto = new SrcSysDto();
		srcSysDto.setApplication_user(requestDto.getHeader().get("user"));
		srcSysDto.setSrc_unique_name(requestDto.getBody().get("data").get("src_sys"));
		
		try {
			 response=dataExtractRepositories.deleteSystem(srcSysDto);
			 
			
			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Source System updated";
			}
			else {
				status="Failed";
				message=response;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status="Failed";
			message=e.getMessage();
		}
		
		// Parse Json to Dto Object
		return ResponseUtil.createResponse(status, message);
	}
	
	
	
	@RequestMapping(value = "/addTableInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addTableInfo(@RequestBody RequestDto requestDto) throws SQLException {
		String status="";
		String message="";
		String response="";
		TableInfoDto tableInfoDto=new TableInfoDto();
		tableInfoDto.setApplication_user(requestDto.getHeader().get("user_name"));
		ArrayList<Map<String,String>> tableInfo=new ArrayList<Map<String,String>>();
		int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
		for(int i=1;i<=counter;i++) {
	    	Map<String,String> tableDetails=new HashMap<String,String>();
	    	tableDetails.put("table_name", requestDto.getBody().get("data").get("table_name"+i));
	    	tableDetails.put("columns", requestDto.getBody().get("data").get("columns_name"+i));
	    	tableDetails.put("where_clause", requestDto.getBody().get("data").get("where_clause"+i));
	    	tableDetails.put("fetch_type", requestDto.getBody().get("data").get("fetch_type"+i));
	    	tableInfo.add(tableDetails);
	    }
		
		
		tableInfoDto.setTableInfo(tableInfo);
		tableInfoDto.setSrc_sys_id(Integer.parseInt(requestDto.getBody().get("data").get("src_sys_id")));
		response=dataExtractRepositories.addTableDetails(tableInfoDto);
		if(response.equalsIgnoreCase("Success")) {
			status="Success";
			message="Table Details Added Successfully";
		}
		else {
			status="Failed";
			message=response;
					
		}
	
		return ResponseUtil.createResponse(status, message);
	}
	
	
	
	@RequestMapping(value = "/addFileInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addFileInfo(@RequestBody UnixDataRequestDto requestDto) throws SQLException, SftpException {
		String status="";
		String message="";
		String response="";
		String put_status="";
		String src_sys_id_str= (String) requestDto.getBody().get("data").get("src_sys_id");
		int src_sys_id=Integer.parseInt(src_sys_id_str);
		ArrayList<Map<String,String>> fileMetadata=(ArrayList<Map<String,String>>) requestDto.getBody().get("data").get("file_details");
		ArrayList<Map<String,String>> fieldMetadata=(ArrayList<Map<String,String>>) requestDto.getBody().get("data").get("field_details");
		ArrayList<FileMetadataDto> fileMetadataArr= new ArrayList<FileMetadataDto>();
		ArrayList<FieldMetadataDto> fieldMetadataArr= new ArrayList<FieldMetadataDto>();
		
		for(Map<String,String> file:fileMetadata) {
			FileMetadataDto fileMetadataDto=new FileMetadataDto();
			fileMetadataDto.setFile_name((String)file.get("file_name"));
			fileMetadataDto.setFile_type((String)file.get("file_type"));
			fileMetadataDto.setFile_delimiter((String)file.get("file_delimiter"));
			fileMetadataDto.setHeader_count((String)file.get("header_count"));
			fileMetadataDto.setTrailer_count((String)file.get("trailer_count"));
			fileMetadataDto.setAvro_conv_flag((String)file.get("avro_conv_flag"));
            fileMetadataArr.add(fileMetadataDto);
		}
		for(Map<String,String> field:fieldMetadata) {
			FieldMetadataDto fieldMetadataDto=new FieldMetadataDto();
			fieldMetadataDto.setFile_name((String)field.get("file_id"));
			fieldMetadataDto.setField_name((String)field.get("field_name"));
			fieldMetadataDto.setField_position((String)field.get("field_pos"));
			fieldMetadataDto.setField_datatype((String)field.get("field_datatype"));
            fieldMetadataArr.add(fieldMetadataDto);
			
		}
            
      FileInfoDto fileInfoDto=new FileInfoDto();
      fileInfoDto.setSrc_sys_id(src_sys_id);
      fileInfoDto.setFileMetadataArr(fileMetadataArr);
      fileInfoDto.setFieldMetadataArr(fieldMetadataArr);
      
		response=dataExtractRepositories.addFileDetails(fileInfoDto);
		if(response.equalsIgnoreCase("Success")) {
			put_status=dataExtractRepositories.putFile(fileInfoDto);
			if(put_status.equalsIgnoreCase("SUCCESS")) {
				status="Success";
				message="File Metadata Added Successfully";
			}
			else {
				status="failed";
				message=put_status;
			}
			
		}
		else {
			status="Failed";
			message=response;
					
		}
	
		return ResponseUtil.createResponse(status, message);
	}
	
	
	
	@RequestMapping(value = "/addHDFSInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addHDFSInfo(@RequestBody RequestDto requestDto) throws SQLException, SftpException {
		String status="";
		String message="";
		String response="";
		ArrayList<String> hdfsFileList=new ArrayList<String>();
		String src_sys_id_str= requestDto.getBody().get("data").get("src_sys_id");

		//int counter= Integer.parseInt(requestDto.getBody().get("data").get("counter"));
		int src_sys_id=Integer.parseInt(src_sys_id_str);
		
		HDFSMetadataDto hdfsDto = new HDFSMetadataDto();
		hdfsDto.setSrc_sys_id(src_sys_id);
		//for(int i=1;i<=counter;i++) {
			hdfsFileList.add(requestDto.getBody().get("data").get("hdfs_path"));
		//}
		hdfsDto.setHdfsPath(hdfsFileList);
            
		response=dataExtractRepositories.addHDFSDetails(hdfsDto);
		if(response.equalsIgnoreCase("Success")) {
				status="success";
				message="HDFS Metadata Added Successfully";
			}
			else {
				status="failed";
			}
		return ResponseUtil.createResponse(status, message);
	}
	
	
	@RequestMapping(value = "/extractData", method = RequestMethod.POST)
	@ResponseBody
	public String dataExtract(@RequestBody RequestDto requestDto) throws UnsupportedOperationException, Exception {
		String response="";
		String status="";
		String message="";
		RealTimeExtractDto rtExtractDto = new RealTimeExtractDto();
		String src_unique_name=requestDto.getBody().get("data").get("src_unique_name");
		//src_sys_id=Integer.parseInt(requestDto.getBody().get("data").get("src_sys_id"));
		rtExtractDto.setConnDto(dataExtractRepositories.getConnectionObject(src_unique_name));
		rtExtractDto.setSrsSysDto(dataExtractRepositories.getSrcSysObject(src_unique_name));
		String targetList=rtExtractDto.getSrsSysDto().getTarget();
		System.out.println("target list is "+targetList);
		rtExtractDto.setTargetArr(dataExtractRepositories.getTargetObject(targetList));
		if(!(rtExtractDto.getSrsSysDto().getTableList()==null||rtExtractDto.getSrsSysDto().getTableList().isEmpty())) {
			String tableList=rtExtractDto.getSrsSysDto().getTableList();
			rtExtractDto.setTableInfoDto(dataExtractRepositories.getTableInfoObject(tableList));
		}
		if(!(rtExtractDto.getSrsSysDto().getFileList()==null||rtExtractDto.getSrsSysDto().getFileList().isEmpty())) {
			if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("UNIX")) {
				String fileList=rtExtractDto.getSrsSysDto().getFileList();
				rtExtractDto.setFileInfoDto(dataExtractRepositories.getFileInfoObject(fileList));
			}
			if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("HADOOP")) {
				String fileList=rtExtractDto.getSrsSysDto().getFileList();
				rtExtractDto.setHdfsInfoDto(dataExtractRepositories.getHDFSInfoObject(fileList));
			}
			
		}
		
		
		
		try {
			 response=dataExtractRepositories.realTimeExtract(rtExtractDto);
			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Data Extraction Initiated Successfully";
			}
			else {
				status="Failed";
				message=response;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status="Failed";
			message=e.getMessage();
		}
		
		// Parse Json to Dto Object
		return ResponseUtil.createResponse(status, message);
		
	}
	
	@RequestMapping(value = "/createDag", method = RequestMethod.POST)
	@ResponseBody
	public String createDag(@RequestBody RequestDto requestDto) throws UnsupportedOperationException, Exception {
		String response="";
		String status="";
		String message="";
		int src_sys_id;
		BatchExtractDto batchExtractDto = new BatchExtractDto();
		String src_unique_name=requestDto.getBody().get("data").get("src_unique_name");
		batchExtractDto.setCron(requestDto.getBody().get("data").get("cron"));
		//src_sys_id=Integer.parseInt(requestDto.getBody().get("data").get("src_sys_id"));
		batchExtractDto.setConnDto(dataExtractRepositories.getConnectionObject(src_unique_name));
		batchExtractDto.setSrsSysDto(dataExtractRepositories.getSrcSysObject(src_unique_name));
		String targetList=batchExtractDto.getSrsSysDto().getTarget();
		batchExtractDto.setTargetArr(dataExtractRepositories.getTargetObject(targetList));
		String tableList=batchExtractDto.getSrsSysDto().getTableList();
		batchExtractDto.setTableInfoDto(dataExtractRepositories.getTableInfoObject(tableList));
		try {
			 response=dataExtractRepositories.batchExtract(batchExtractDto);
			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Batch Scheduled Successfully";
			}
			else {
				status="Failed";
				message=response;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status="Failed";
			message=e.getMessage();
		}
		
		// Parse Json to Dto Object
		return ResponseUtil.createResponse(status, message);
		
	}
}
