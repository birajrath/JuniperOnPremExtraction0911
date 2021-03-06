/**
 * 
 */
package com.dataextract.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.dataextract.dto.ConnectionDto;
import com.dataextract.dto.FeedDto;
import com.dataextract.dto.FieldMetadataDto;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.FileMetadataDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.HiveDbMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.RequestDto;
import com.dataextract.dto.ScheduleExtractDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TableMetadataDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.dto.TempTableInfoDto;
import com.dataextract.dto.TempTableMetadataDto;
import com.dataextract.dto.UnixDataRequestDto;
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
	public String addConnection(@RequestBody RequestDto requestDto) throws Exception {
		// Parse json to Dto Object
		String testConnStatus="";
		String response;
		String status = "";
		String message = "";
		ConnectionDto connDto = new ConnectionDto();


		if(requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("ORACLE")
				||requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("HADOOP")
				||requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("TERADATA")
				||requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("MSSQL")) 
		{
			
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
			connDto.setHostName(requestDto.getBody().get("data").get("host_name"));
			connDto.setPort(requestDto.getBody().get("data").get("port"));
			connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
			connDto.setPassword(requestDto.getBody().get("data").get("password"));
			connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
			connDto.setServiceName(requestDto.getBody().get("data").get("service_name"));
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
			connDto.setProject(requestDto.getBody().get("data").get("project"));
			connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
			testConnStatus=dataExtractRepositories.testConnection(connDto);
			if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
				response = dataExtractRepositories.addConnectionDetails(connDto);
				if(response.toLowerCase().contains("success")) {
					status="Success";
					message="Connection created with Connection Id "+response.split(":")[1];
				}
				else {
					status="Failed";
					message=response;
				}

			}
			else {
				status="Failed";
				message=testConnStatus;
			}

		}

		if(requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("UNIX")) 
		{
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
			connDto.setDrive_id(Integer.parseInt(requestDto.getBody().get("data").get("drive_id")));
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
			connDto.setProject(requestDto.getBody().get("data").get("project"));
			connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
			testConnStatus=dataExtractRepositories.testConnection(connDto);
			if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
				response = dataExtractRepositories.addConnectionDetails(connDto);
				if(response.toLowerCase().contains("success")) {
					status="Success";
					message="Connection created with Connection Id "+response.split(":")[1];
				}
				else {
					status="Failed";
					message=response;
				}

			}
			else {
				status="Failed";
				message=testConnStatus;
			}

		}
	
		if(requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("HIVE")) 
		{
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
			connDto.setHostName(requestDto.getBody().get("data").get("knox_host_name"));
			connDto.setPort(requestDto.getBody().get("data").get("knox_port"));
			connDto.setKnox_gateway(requestDto.getBody().get("data").get("knox_gateway"));
			connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
			connDto.setPassword(requestDto.getBody().get("data").get("password"));
			connDto.setTrust_store_path(requestDto.getBody().get("data").get("ts_path"));
			connDto.setTrust_store_password(requestDto.getBody().get("data").get("ts_password"));
			connDto.setProject(requestDto.getBody().get("data").get("project"));
			connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
			testConnStatus=dataExtractRepositories.testHiveConnection(connDto);
			if(!(testConnStatus.equalsIgnoreCase("FAILED"))) {
				response = dataExtractRepositories.addHiveConnectionDetails(connDto,testConnStatus);
				if(response.toLowerCase().contains("success")) {
					status="Success";
					message="Connection created with Connection Id "+response.split(":")[1];
				}
				else {
					status="Failed";
					message=response;
				}

			}
			else {
				status="Failed";
				message="Erroe while establishing Connection to Hive";
			}

		}
		

		
		return ResponseUtil.createResponse(status, message);
	}


	@RequestMapping(value = "/updConnection", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updConnection(@RequestBody RequestDto requestDto) throws Exception {
		// Parse json to Dto Object
		String testConnStatus="";
		String status = "";
		String message = "";
		String response="";
		ConnectionDto connDto = new ConnectionDto();


		connDto.setConnId(Integer.parseInt(requestDto.getBody().get("data").get("conn")));
		connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
		connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
		connDto.setSystem(requestDto.getBody().get("data").get("system"));

		if(connDto.getConn_type().equalsIgnoreCase("ORACLE")
				||connDto.getConn_type().equalsIgnoreCase("HADOOP")
				||requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("TERADATA"))  {

			connDto.setHostName(requestDto.getBody().get("data").get("host_name"));
			connDto.setPort(requestDto.getBody().get("data").get("port"));
			connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
			connDto.setPassword(requestDto.getBody().get("data").get("password"));
			connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
			connDto.setServiceName(requestDto.getBody().get("data").get("service_name"));
			connDto.setProject(requestDto.getBody().get("data").get("project"));
			connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		}
		if(connDto.getConn_type().equalsIgnoreCase("UNIX")){

			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setDrive_id(Integer.parseInt(requestDto.getBody().get("data").get("drive_id")));
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
			connDto.setProject(requestDto.getBody().get("data").get("project"));
			connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));

		}
		if(requestDto.getBody().get("data").get("connection_type").equalsIgnoreCase("HIVE")) 
		{
			connDto.setSystem(requestDto.getBody().get("data").get("system"));
			connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
			connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
			connDto.setHostName(requestDto.getBody().get("data").get("knox_host_name"));
			connDto.setPort(requestDto.getBody().get("data").get("knox_port"));
			connDto.setKnox_gateway(requestDto.getBody().get("data").get("knox_gateway"));
			connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
			connDto.setPassword(requestDto.getBody().get("data").get("password"));
			connDto.setTrust_store_path(requestDto.getBody().get("data").get("ts_path"));
			connDto.setTrust_store_password(requestDto.getBody().get("data").get("ts_password"));
			connDto.setProject(requestDto.getBody().get("data").get("project"));
			connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		}
		testConnStatus=dataExtractRepositories.testConnection(connDto);
		if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
			response = dataExtractRepositories.updateConnectionDetails(connDto);
			if(response.toLowerCase().contains("success")){
				status="Success";
				message="Details updated Successfully";
			}
			else {
				status="Failed";
				message=response;
			}
		}
		else {
			status="Failed";
			message=testConnStatus;
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
			message = "Connection deleted Successfully";
		} 
		return ResponseUtil.createResponse(status, message);
	}



	@RequestMapping(value = "/addTarget", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addTarget(@RequestBody RequestDto requestDto) throws SQLException {
		// Parse json to Dto Object
		String response="";
		String status = "";
		String message = "";
		TargetDto target=new TargetDto();
		if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("gcs")){
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			target.setTarget_project(requestDto.getBody().get("data").get("target_project"));
			target.setService_account(requestDto.getBody().get("data").get("service_account"));
			target.setService_account_key(requestDto.getBody().get("data").get("service_account_key"));
			target.setTarget_bucket(requestDto.getBody().get("data").get("target_bucket"));
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
			target.setProject(requestDto.getBody().get("data").get("project"));

		}
		if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("hdfs")){
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			String knox_host=requestDto.getBody().get("data").get("knox_host").trim();
			if(!(knox_host.contains("https://"))) {
				knox_host="https://"+knox_host;
			}
			target.setTarget_knox_host(knox_host);
			target.setTarget_knox_port(Integer.parseInt(requestDto.getBody().get("data").get("knox_port")));
			target.setTarget_hdfs_gateway(requestDto.getBody().get("data").get("hdfs_gateway"));
			target.setTarget_hdfs_path(requestDto.getBody().get("data").get("hadoop_path"));
			target.setMaterialization_flag(requestDto.getBody().get("data").get("materialization_flag"));
			target.setPartition_flag(requestDto.getBody().get("data").get("partition_flag"));
			target.setHive_gateway(requestDto.getBody().get("data").get("hive_gateway"));
			target.setTarget_user(requestDto.getBody().get("data").get("username"));
			target.setTarget_password(requestDto.getBody().get("data").get("password"));
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
			target.setProject(requestDto.getBody().get("data").get("project"));
		}
		if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("unix")){

			System.out.println("unix target");
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			target.setDrive_id(requestDto.getBody().get("data").get("drive_id"));
			target.setData_path(requestDto.getBody().get("data").get("data_path"));
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
			target.setProject(requestDto.getBody().get("data").get("project"));
		}
		/*if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("HIVE")) 
		{
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			target.setTarget_knox_url(requestDto.getBody().get("data").get("knox_url"));
			target.setTarget_user(requestDto.getBody().get("data").get("target_user"));
			target.setTarget_password(requestDto.getBody().get("data").get("password"));
			target.setKnox_gateway(requestDto.getBody().get("data").get("knox_gateway"));
			target.setTrust_store_path(requestDto.getBody().get("data").get("ts_path"));
			target.setTrust_store_password(requestDto.getBody().get("data").get("ts_password"));
			target.setProject(requestDto.getBody().get("data").get("project"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
		}*/
		response = dataExtractRepositories.addTargetDetails(target);
		if(response.toLowerCase().contains("success")) {
			status="Success";
			message="Target Added: Target Id is "+response.split(":")[1];

		}
		else {
			status="Failed";
			message=response;
		}
		return ResponseUtil.createResponse(status, message);
	}


	@RequestMapping(value = "/updTarget", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updTarget(@RequestBody RequestDto requestDto) throws SQLException {

		String response="";
		String status = "";
		String message = "";

		TargetDto target=new TargetDto();



		if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("gcs")){
			target.setTarget_id(Integer.parseInt(requestDto.getBody().get("data").get("tgt")));
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			target.setTarget_project(requestDto.getBody().get("data").get("target_project"));
			target.setService_account(requestDto.getBody().get("data").get("service_account"));
			target.setTarget_bucket(requestDto.getBody().get("data").get("target_bucket"));
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setProject(requestDto.getBody().get("data").get("project"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
		}
		if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("hdfs")){
			target.setTarget_id(Integer.parseInt(requestDto.getBody().get("data").get("tgt")));
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			target.setTarget_knox_host(requestDto.getBody().get("data").get("knox_host"));
			target.setTarget_knox_port(Integer.parseInt(requestDto.getBody().get("data").get("knox_port")));
			target.setTarget_hdfs_gateway(requestDto.getBody().get("data").get("hdfs_gateway"));
			target.setTarget_user(requestDto.getBody().get("data").get("username"));
			target.setTarget_password(requestDto.getBody().get("data").get("password"));
			target.setTarget_hdfs_path(requestDto.getBody().get("data").get("hadoop_path"));
			target.setMaterialization_flag(requestDto.getBody().get("data").get("materialization_flag"));
			target.setPartition_flag(requestDto.getBody().get("data").get("partition_flag"));
			target.setHive_gateway(requestDto.getBody().get("data").get("hive_gateway"));
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setProject(requestDto.getBody().get("data").get("project"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
		}
		if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("unix")){
			target.setTarget_id(Integer.parseInt(requestDto.getBody().get("data").get("tgt")));
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			target.setDrive_id(requestDto.getBody().get("data").get("drive_id"));
			target.setData_path(requestDto.getBody().get("data").get("data_path"));
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setProject(requestDto.getBody().get("data").get("project"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
		}
		
		/*if(requestDto.getBody().get("data").get("target_type").equalsIgnoreCase("HIVE")) 
		{
			target.setSystem(requestDto.getBody().get("data").get("system"));
			target.setTarget_unique_name(requestDto.getBody().get("data").get("target_unique_name"));
			target.setTarget_type(requestDto.getBody().get("data").get("target_type"));
			target.setTarget_knox_url(requestDto.getBody().get("data").get("knox_url"));
			target.setTarget_user(requestDto.getBody().get("data").get("target_user"));
			target.setTarget_password(requestDto.getBody().get("data").get("password"));
			target.setKnox_gateway(requestDto.getBody().get("data").get("knox_gateway"));
			target.setTrust_store_path(requestDto.getBody().get("data").get("ts_path"));
			target.setTrust_store_password(requestDto.getBody().get("data").get("ts_password"));
			target.setProject(requestDto.getBody().get("data").get("project"));
			target.setJuniper_user(requestDto.getBody().get("data").get("user"));
		}*/

		response = dataExtractRepositories.updateTargetDetails(target);
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
	public String onBoardSystem(@RequestBody RequestDto requestDto) throws SQLException {
		String response="";
		String status="";
		String message="";

		FeedDto feedDto = new FeedDto();
		feedDto.setFeed_name(requestDto.getBody().get("data").get("feed_name"));
		feedDto.setSrc_conn_id(Integer.parseInt(requestDto.getBody().get("data").get("src_connection_id")));
		feedDto.setFeed_desc(requestDto.getBody().get("data").get("feed_desc"));
		feedDto.setCountry_code(requestDto.getBody().get("data").get("country_code"));
		feedDto.setFeed_extract_type(requestDto.getBody().get("data").get("feed_extract_type"));
		feedDto.setTarget(requestDto.getBody().get("data").get("target"));
		feedDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		feedDto.setProject(requestDto.getBody().get("data").get("project"));

		response=dataExtractRepositories.onboardSystem(feedDto);
		if(response.toLowerCase().contains("success")) {
			status="Success";
			message="Feed created with Feed Id-"+response.split(":")[1];
		}
		else {
			status="Failed";
			message=response;
		}
		return ResponseUtil.createResponse(status, message);
	}


	@RequestMapping(value = "/updSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updSystem(@RequestBody RequestDto requestDto) throws SQLException{
		String response="";
		String status="";
		String message="";
		FeedDto feedDto = new FeedDto();

		feedDto.setFeed_name(requestDto.getBody().get("data").get("feed_name"));
		feedDto.setSrc_conn_id(Integer.parseInt(requestDto.getBody().get("data").get("src_connection_id")));
		feedDto.setFeed_desc(requestDto.getBody().get("data").get("feed_desc"));
		feedDto.setCountry_code(requestDto.getBody().get("data").get("country_code"));
		feedDto.setFeed_extract_type(requestDto.getBody().get("data").get("feed_extract_type"));
		feedDto.setTarget(requestDto.getBody().get("data").get("target"));
		feedDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("feed")));
		feedDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		feedDto.setProject(requestDto.getBody().get("data").get("project"));
		response=dataExtractRepositories.updateFeed(feedDto);
		if(response.equalsIgnoreCase("success")) {
			status="Success";
			message="Feed updated";
		}
		else {
			status="Failed";
			message=response;
		}


		return ResponseUtil.createResponse(status, message);
	}

	@RequestMapping(value = "/delSystem", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String delSystem(@RequestBody RequestDto requestDto) {
		String response="";
		String status="";
		String message="";
		FeedDto feedDto = new FeedDto();

		feedDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("feed")));

		try {
			response=dataExtractRepositories.deleteFeed(feedDto);


			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Feed deleted";
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

		ArrayList<TableMetadataDto> tableMetadataArr=new ArrayList<TableMetadataDto>();
		int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
		String load_type=requestDto.getBody().get("data").get("load_type");
		
		
		for(int i=1;i<=counter;i++) {
			TableMetadataDto tableMetadata=new TableMetadataDto();
			
			tableMetadata.setTable_name(requestDto.getBody().get("data").get("table_name"+i).toUpperCase());
			String view_flag=requestDto.getBody().get("data").get("view_flag"+i);
			if(view_flag==null|| view_flag.isEmpty()) {
				tableMetadata.setView_flag("N");
			}
			else {
				tableMetadata.setView_flag(view_flag);
				tableMetadata.setView_source_schema(requestDto.getBody().get("data").get("view_src_schema"+i));
			}
			
			
			tableMetadata.setColumns(requestDto.getBody().get("data").get("columns_name"+i).toUpperCase());
			tableMetadata.setAll_cols(requestDto.getBody().get("data").get("all_columns_name"+i).toUpperCase());
			tableMetadata.setWhere_clause(requestDto.getBody().get("data").get("where_clause"+i));
			tableMetadata.setFetch_type(requestDto.getBody().get("data").get("fetch_type"+i));
			tableMetadata.setIncr_col(requestDto.getBody().get("data").get("incr_col"+i));
			tableMetadataArr.add(tableMetadata);

		}
		
		if(load_type ==null || load_type.isEmpty()) {
			tableInfoDto.setLoad_type("ind");
		}
		else {
			tableInfoDto.setLoad_type(load_type);
		}
		
		tableInfoDto.setTableMetadataArr(tableMetadataArr);
		tableInfoDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		tableInfoDto.setProject(requestDto.getBody().get("data").get("project"));
		tableInfoDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("feed_id")));
		response=dataExtractRepositories.addTableDetails(tableInfoDto);
		if(response.toLowerCase().contains("success")) {
			status="Success";
			message="Table Details Added Successfully. Table IDs are "+response.split(":")[1];
		}
		else {
			status="Failed";
			message=response;

		}

		return ResponseUtil.createResponse(status, message);
	}
	
	@RequestMapping(value = "/addTempTableInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addTempTableInfo(@RequestBody RequestDto requestDto) throws SQLException {
		String status="";
		String message="";
		String response="";
		
		//String load_type=requestDto.getBody().get("data").get("load_type");
		String feed_id=requestDto.getBody().get("data").get("feed_id");
		String src_type=requestDto.getBody().get("data").get("src_type");
		
		response=dataExtractRepositories.editTempTableDetails(feed_id,src_type);
	
		if(response.toLowerCase().contains("success")) {
			TempTableInfoDto tempTableInfoDto=new TempTableInfoDto();

			ArrayList<TempTableMetadataDto> tempTableMetadataArr=new ArrayList<TempTableMetadataDto>();
			int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
			String load_type=requestDto.getBody().get("data").get("load_type");
			
			
			for(int i=1;i<=counter;i++) {
				TempTableMetadataDto tableMetadata=new TempTableMetadataDto();
				
				tableMetadata.setTable_name(requestDto.getBody().get("data").get("table_name"+i).toUpperCase());
				String view_flag=requestDto.getBody().get("data").get("view_flag"+i);
				if(view_flag==null|| view_flag.isEmpty()) {
					tableMetadata.setView_flag("N");
				}
				else {
					tableMetadata.setView_flag(view_flag);
					tableMetadata.setView_source_schema(requestDto.getBody().get("data").get("view_src_schema"+i));
				}
				
				
				tableMetadata.setColumns(requestDto.getBody().get("data").get("columns_name"+i).toUpperCase());
				tableMetadata.setWhere_clause(requestDto.getBody().get("data").get("where_clause"+i));
				tableMetadata.setFetch_type(requestDto.getBody().get("data").get("fetch_type"+i));
				tableMetadata.setIncr_col(requestDto.getBody().get("data").get("incr_col"+i));
				tempTableMetadataArr.add(tableMetadata);

			}
			
			if(load_type ==null || load_type.isEmpty()) {
				tempTableInfoDto.setLoad_type("ind");
			}
			else {
				tempTableInfoDto.setLoad_type(load_type);
			}
			
			tempTableInfoDto.setTableMetadataArr(tempTableMetadataArr);
			tempTableInfoDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
			tempTableInfoDto.setProject(requestDto.getBody().get("data").get("project"));
			tempTableInfoDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("feed_id")));
			response=dataExtractRepositories.addTempTableDetails(tempTableInfoDto);
			if(response.toLowerCase().contains("success")) {
				status="Success";
				message="Table Details Added Successfully. Table IDs are "+response.split(":")[1];
			}
			else {
				status="Failed";
				message=response;
			}
		}else {
			status="Failed";
			message=response;
		}
		return ResponseUtil.createResponse(status, message);
	}
	
	@RequestMapping(value = "/editTempTableInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String editTempTableInfo(@RequestBody RequestDto requestDto) throws SQLException {
		String status="";
		String message="";
		String response="";
		
		//String load_type=requestDto.getBody().get("data").get("load_type");
		String feed_id=requestDto.getBody().get("data").get("feed_id");
		String src_type=requestDto.getBody().get("data").get("src_type");
		
		response=dataExtractRepositories.editTempTableDetails(feed_id,src_type);
	
		if(response.toLowerCase().contains("success")) {
			TempTableInfoDto tempTableInfoDto=new TempTableInfoDto();

			ArrayList<TempTableMetadataDto> tempTableMetadataArr=new ArrayList<TempTableMetadataDto>();
			int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
			String load_type=requestDto.getBody().get("data").get("load_type");
			
			
			for(int i=1;i<=counter;i++) {
				TempTableMetadataDto tableMetadata=new TempTableMetadataDto();
				
				tableMetadata.setTable_name(requestDto.getBody().get("data").get("table_name"+i).toUpperCase());
				String view_flag=requestDto.getBody().get("data").get("view_flag"+i);
				if(view_flag==null|| view_flag.isEmpty()) {
					tableMetadata.setView_flag("N");
				}
				else {
					tableMetadata.setView_flag(view_flag);
					tableMetadata.setView_source_schema(requestDto.getBody().get("data").get("view_src_schema"+i));
				}
				
				
				tableMetadata.setColumns(requestDto.getBody().get("data").get("columns_name"+i).toUpperCase());
				tableMetadata.setWhere_clause(requestDto.getBody().get("data").get("where_clause"+i));
				tableMetadata.setFetch_type(requestDto.getBody().get("data").get("fetch_type"+i));
				tableMetadata.setIncr_col(requestDto.getBody().get("data").get("incr_col"+i));
				tempTableMetadataArr.add(tableMetadata);

			}
			
			if(load_type ==null || load_type.isEmpty()) {
				tempTableInfoDto.setLoad_type("ind");
			}
			else {
				tempTableInfoDto.setLoad_type(load_type);
			}
			
			tempTableInfoDto.setTableMetadataArr(tempTableMetadataArr);
			tempTableInfoDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
			tempTableInfoDto.setProject(requestDto.getBody().get("data").get("project"));
			tempTableInfoDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("feed_id")));
			response=dataExtractRepositories.addTempTableDetails(tempTableInfoDto);
			if(response.toLowerCase().contains("success")) {
				status="Success";
				message="Table Details Added Successfully. Table IDs are "+response.split(":")[1];
			}
			else {
				status="Failed";
				message=response;
			}
		}else {
			status="Failed";
			message=response;
		}
		return ResponseUtil.createResponse(status, message);
	}


	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/addFileInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addFileInfo(@RequestBody UnixDataRequestDto requestDto) throws SQLException, SftpException {
		String status="";
		String message="";
		String response="";
		String feed_id_str= (String) requestDto.getBody().get("data").get("feed_id");
		String dataPath=(String) requestDto.getBody().get("data").get("data_path");
		String project=(String) requestDto.getBody().get("data").get("project");
		String user=(String) requestDto.getBody().get("data").get("user");
		int feed_id=Integer.parseInt(feed_id_str);
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
			fileMetadataDto.setBus_dt_format((String)file.get("bus_dt_format"));
			fileMetadataDto.setBus_dt_loc((String)(file.get("bus_dt_loc")));
			String bus_dt_start=file.get("bus_dt_start");
			if(!(bus_dt_start==null || bus_dt_start.isEmpty())) {
				fileMetadataDto.setBus_dt_start(Integer.parseInt(file.get("bus_dt_start")));
			}
			fileMetadataDto.setCount_loc((String)file.get("count_loc"));
			String count_start=file.get("count_start");
			if(!(count_start==null||count_start.isEmpty())) {
				fileMetadataDto.setCount_start(Integer.parseInt(file.get("count_start")));
			}
			String count_length=file.get("count_legnth");
			if(!(count_length==null||count_length.isEmpty())) {
				fileMetadataDto.setCount_legnth(Integer.parseInt(file.get("count_legnth")));
			}

			fileMetadataArr.add(fileMetadataDto);
		}
		for(Map<String,String> field:fieldMetadata) {
			FieldMetadataDto fieldMetadataDto=new FieldMetadataDto();
			fieldMetadataDto.setFile_name((String)field.get("file_id"));
			fieldMetadataDto.setField_name((String)field.get("field_name"));
			String field_pos=field.get("field_pos");
			if(!(field_pos==null||field_pos.isEmpty())) {
				fieldMetadataDto.setField_position(Integer.parseInt(field_pos));
			}

			String length=(String)field.get("length");
			if(!(length==null||length.isEmpty())) {
				fieldMetadataDto.setLength(Integer.parseInt(field.get("length")));
			}
			fieldMetadataDto.setField_datatype((String)field.get("field_datatype"));
			fieldMetadataArr.add(fieldMetadataDto);

		}

		FileInfoDto fileInfoDto=new FileInfoDto();
		fileInfoDto.setFeed_id(feed_id);
		fileInfoDto.setDataPath(dataPath);
		fileInfoDto.setProject(project);
		fileInfoDto.setJuniper_user(user);
		fileInfoDto.setFileMetadataArr(fileMetadataArr);
		fileInfoDto.setFieldMetadataArr(fieldMetadataArr);

		response=dataExtractRepositories.addFileDetails(fileInfoDto);
		if(response.equalsIgnoreCase("Success")) {

			status="Success";
			message="File Metadata Added Successfully";
		}
		else {
			status="failed";
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
	
	
	
	@RequestMapping(value = "/addHivePropagateDbInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addHivePropagateDatabaseInfo(@RequestBody RequestDto requestDto) throws SQLException, SftpException {
		String status="";
		String message="";
		String response="";
		
		HiveDbMetadataDto hivedbDto = new HiveDbMetadataDto();
		String src_sys_id_str= requestDto.getBody().get("data").get("feed_id");
		int src_sys_id=Integer.parseInt(src_sys_id_str);
		hivedbDto.setFeed_id(src_sys_id);
		String project=(String) requestDto.getBody().get("data").get("project");
		hivedbDto.setProject(project);
		String user=(String) requestDto.getBody().get("data").get("user");
		hivedbDto.setJuniper_user(user);
		String hiveDbString=requestDto.getBody().get("data").get("hive_db_list");
		List<String> hiveDbList = Arrays.asList(hiveDbString.split(","));
		hivedbDto.setHiveDbList(hiveDbList);
		response=dataExtractRepositories.addHivePropagateDbDetails(hivedbDto);
		if(response.equalsIgnoreCase("Success")) {
			status="success";
			message="Hive Metadata Added Successfully";
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
		String feed_name=requestDto.getBody().get("data").get("feed_name");
		String encryption=requestDto.getBody().get("data").get("encryption");
		if(!(encryption==null||encryption.isEmpty())) {
			rtExtractDto.setEncryption_flag("Y");
		}
		try {
			
			rtExtractDto.setConnDto(dataExtractRepositories.getConnectionObject(feed_name));
			System.out.println("connection details retrieved");
			rtExtractDto.setFeedDto(dataExtractRepositories.getFeedObject(feed_name,rtExtractDto.getConnDto().getConn_type()));
			String targetList=rtExtractDto.getFeedDto().getTarget();
			rtExtractDto.setTargetArr(dataExtractRepositories.getTargetObject(targetList));
			if(!(rtExtractDto.getFeedDto().getTableList()==null||rtExtractDto.getFeedDto().getTableList().isEmpty())) {
				
				String tableList=rtExtractDto.getFeedDto().getTableList();
				rtExtractDto.setTableInfoDto(dataExtractRepositories.getTableInfoObject(tableList));
				
			}
			
			if(!(rtExtractDto.getFeedDto().getDbList()==null||rtExtractDto.getFeedDto().getDbList().isEmpty())) {
				
				String dbList=rtExtractDto.getFeedDto().getDbList();
				rtExtractDto.setHiveInfoDto(dataExtractRepositories.getHivePropagateInfoObject(dbList));
			}
			if(!(rtExtractDto.getFeedDto().getFileList()==null||rtExtractDto.getFeedDto().getFileList().isEmpty())) {
				
					String fileList=rtExtractDto.getFeedDto().getFileList();
					rtExtractDto.setFileInfoDto(dataExtractRepositories.getFileInfoObject(fileList));
				
			}
			
		}catch(Exception e) {
			status="failed";
			message=e.getMessage();
			return ResponseUtil.createResponse(status, message);
		}
		

		try {
			response=dataExtractRepositories.realTimeExtract(rtExtractDto);
			if(response.equalsIgnoreCase("success")) {
				status="Success";
				message="Data Extraction Initiated Successfully";
				return ResponseUtil.createResponse(status, message);

			}
			else {
				status="Failed";
				message=response;
				return ResponseUtil.createResponse(status, message);

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status="Failed";
			message=e.getMessage();
			return ResponseUtil.createResponse(status, message);

		}

		// Parse Json to Dto Object
		
	}

	@RequestMapping(value = "/createDag", method = RequestMethod.POST)
	@ResponseBody
	public String createDag(@RequestBody RequestDto requestDto) throws UnsupportedOperationException, Exception {
		String response="";
		String status="";
		String message="";
		ScheduleExtractDto schDto=new ScheduleExtractDto();
		schDto.setFeed_name(requestDto.getBody().get("data").get("feed_name"));
		schDto.setSch_flag(requestDto.getBody().get("data").get("sch_flag"));
		schDto.setExtraction_mode(requestDto.getBody().get("data").get("extraction_mode"));
		
		if(schDto.getSch_flag().equalsIgnoreCase("R")) {
			schDto.setCron(requestDto.getBody().get("data").get("cron"));
		}
		if(schDto.getSch_flag().equalsIgnoreCase("F")) {
			schDto.setFile_path(requestDto.getBody().get("data").get("file_path"));
			schDto.setFile_pattern(requestDto.getBody().get("data").get("file_pattern"));
		}
		
		if(schDto.getSch_flag().equalsIgnoreCase("K")) {
			
			schDto.setKafka_topic(requestDto.getBody().get("data").get("kafka_topic"));
			
		}
		if(schDto.getSch_flag().equalsIgnoreCase("A")) {
			
			
			schDto.setApi_unique_key(requestDto.getBody().get("data").get("api_unique_key"));
			
		}

		try {
			response=dataExtractRepositories.batchExtract(schDto);
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
	
	@RequestMapping(value = "/metaDataValidation", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String metaDataValidation(@RequestBody RequestDto requestDto) throws SQLException {
		System.out.println("Reached inside Meta data validation block");
		String status="";
		String message="";
		String response="";
		System.out.println(requestDto.toString());
		String feed_sequence=requestDto.getBody().get("data").get("feed_sequence");
		String project_id=requestDto.getBody().get("data").get("project_id");
		
		System.out.println("feed_sequence is "+feed_sequence);	
		System.out.println("project_id is "+project_id);
		
		response=dataExtractRepositories.metaDataValidate(feed_sequence,project_id);
		if(response.contains("success")) {			
			status="success";
			message="Metadata validated Successfully";
		}
		else {
			status="Failed";
			message=response;
		}		
		return ResponseUtil.createResponse(status, message);
	}
	
	
	
}
