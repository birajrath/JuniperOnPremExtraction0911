package com.dataextract.fetchdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import com.dataextract.constants.NifiConstants;
import com.dataextract.dao.IExtractionDAO;
import com.dataextract.dto.FileMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.TableMetadataDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.repositories.DataExtractRepositories;


@Repository
public class ExtractNifiImpl implements IExtract {


	@Autowired
	private DataExtractRepositories dataExtractRepositories;
	
	@Autowired
	private IExtractionDAO iExtract;
	

	@SuppressWarnings("static-access")
	@Override
	public  String callNifiDataRealTime(RealTimeExtractDto rtExtractDto,String conn_string,String date,Long runId) throws UnsupportedOperationException, Exception {
		
		String processGroupUrl="";
		String listenHttpUrl="";
		String controllerId="";
		String clientId="";
		String controllerVersion="";
		String controllerStatus="";
		int index=0;
		String processorInfo="";
		HttpEntity respEntity=null;
		int processGroupIndex=0;
		String trigger_flag="N";
		String processGroupStatus="";
		String path="";
	
		
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE")) 
		{
		
			if(rtExtractDto.getTableInfoDto().getIncr_flag().equalsIgnoreCase("Y")) {
				processGroupIndex=dataExtractRepositories.getProcessGroup(rtExtractDto.getFeedDto().getFeed_name(),rtExtractDto.getFeedDto().getCountry_code());
				
			}
		
			do {
				
				Thread.currentThread().sleep(10000);
				if(processGroupIndex==0) {
					
					index=getRandomNumberInRange(1, NifiConstants.NOOFORACLEPROCESSORS);
				}
				else {
					index=processGroupIndex;
				}
				processGroupStatus=dataExtractRepositories.checkProcessGroupStatus(index,rtExtractDto.getConnDto().getConn_type());
				if(processGroupStatus.equalsIgnoreCase("FREE")) {
					
					NifiConstants constants=new NifiConstants();
					String varName="ORACLEPROCESSGROUPURL"+index;
					processGroupUrl = String.valueOf(NifiConstants.class.getDeclaredField(varName).get(constants));
					listenHttpUrl= NifiConstants.ORACLELISTENER1;
					respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
					if (respEntity != null) {
						String content = EntityUtils.toString(respEntity);
						JSONObject controllerObj=getControllerObject(content);
						String controllerInfo=getControllerInfo(controllerObj);
						controllerId=controllerInfo.split(",")[0];
						clientId=controllerInfo.split(",")[1];
						controllerVersion=controllerInfo.split(",")[2];
						controllerStatus=controllerInfo.split(",")[3];
						System.out.println("controller id,clientId, version and status are : "+controllerId+" "+clientId+" "+controllerVersion+" "+controllerStatus);
						if(controllerStatus.equalsIgnoreCase("ENABLED")) {
							processorInfo=processorFree(controllerObj);
							if(!processorInfo.equalsIgnoreCase("NOT FREE")) {
								System.out.println("using processgroup"+index);
								System.out.println("processors being used are: "+processorInfo);
								trigger_flag="Y";
								
							}
						}
					}
				}
			}while(trigger_flag.equalsIgnoreCase("N"));	
			
		}
		
		 if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("TERADATA")) {
				
				for(int i =0;i<100;i++) {
					NifiConstants constants=new NifiConstants();
					index=getRandomNumberInRange(1, NifiConstants.NOOFTERADATAPROCESSORS);
					String varName="TERADATAPROCESSGROUPURL"+index;
					processGroupUrl = String.valueOf(NifiConstants.class.getDeclaredField(varName).get(constants));
					//String listener="ORACLELISTENER"+index;
					listenHttpUrl= NifiConstants.TERADATALISTENER1;
					System.out.println("process group url is "+processGroupUrl);
					respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
					if (respEntity != null) {
						String content = EntityUtils.toString(respEntity);
						JSONObject controllerObj=getControllerObject(content);
						String controllerInfo=getControllerInfo(controllerObj);
						controllerId=controllerInfo.split(",")[0];
						clientId=controllerInfo.split(",")[1];
						controllerVersion=controllerInfo.split(",")[2];
						controllerStatus=controllerInfo.split(",")[3];
						System.out.println("controller id,clientId, version and status are : "+controllerId+" "+clientId+" "+controllerVersion+" "+controllerStatus);
						if(controllerStatus.equalsIgnoreCase("ENABLED")) {
							processorInfo=processorFree(controllerObj);
							if(!processorInfo.equalsIgnoreCase("NOT FREE")) {
								System.out.println("using processgroup"+index);
								System.out.println("processors being used are: "+processorInfo);
								trigger_flag="Y";
							}
						}
					}
				}
			}
		 
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE")||rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("TERADATA")) {
			
			stopReferencingComponents(processorInfo, clientId);
			disableController(controllerId);
			updateController(conn_string , rtExtractDto.getConnDto().getUserName(), rtExtractDto.getConnDto().getEncr_key(), rtExtractDto.getConnDto().getEncrypted_password(), controllerId);
			
			enableController(controllerId);
			startReferencingComponents(controllerId,processGroupUrl);
			path=rtExtractDto.getFeedDto().getCountry_code()+"/"+ rtExtractDto.getFeedDto().getFeed_name()+"/"+date+"/"+runId+"/";
			JSONArray arr=createJsonObject(index,rtExtractDto,conn_string,path,date, runId);
			invokeNifiFull(arr,listenHttpUrl);
		}
		
		String updateStatus=dataExtractRepositories.updateNifiProcessgroupDetails(rtExtractDto,path, date, runId.toString(), index);
		if(updateStatus.equalsIgnoreCase("success")) {
			return "success";
		}
		else {
			return "failed";
		}
		
	
	}
	
	@SuppressWarnings("static-access")
	@Override
	public String callNifiUnixRealTime(RealTimeExtractDto rtExtractDto, String date, Long runId) throws UnsupportedOperationException, Exception {
		int index=0;
		String path="";
		String trigger_flag="N";
		String processGroupStatus="";
		do {
			Thread.currentThread().sleep(10000);
			
			index=getRandomNumberInRange(1, NifiConstants.NOOFUNIXPROCESSORS);
			processGroupStatus=dataExtractRepositories.checkProcessGroupStatus(index,rtExtractDto.getConnDto().getConn_type());
			if(processGroupStatus.equalsIgnoreCase("FREE")) {
				
				trigger_flag="Y";
				
			}
			
		}while(trigger_flag.equalsIgnoreCase("N"));
		
		String listener=NifiConstants.UNIXLISTENER1;
		path=rtExtractDto.getFeedDto().getCountry_code()+"/"+ rtExtractDto.getFeedDto().getFeed_name()+"/"+date+"/"+runId+"/";
		JSONArray jsonArr=createUnixJsonObject(rtExtractDto,path,index,date, runId);
		invokeNifiFull(jsonArr,listener);
		
		String updateStatus=dataExtractRepositories.updateNifiProcessgroupDetails(rtExtractDto,path, date, runId.toString(), index);
		if(updateStatus.equalsIgnoreCase("success")) {
			return "success";
		}
		else {
			return "failed";
		}
		
		
	}
	
	@Override
	public String callNifiHadoopRealTime(RealTimeExtractDto rtExtractDto, String date, Long runId) throws UnsupportedOperationException, Exception {
		String listener=NifiConstants.HADOOPLISTENER1;
		JSONArray jsonArr=createHadoopJsonObject(rtExtractDto,date, runId);
		invokeNifiFull(jsonArr,listener);
		return "success";
		
	}
	
	
	

	private  int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
			}
			Random r = new Random();
			return r.nextInt((max - min) + 1) + min;
	}
	
	private  HttpEntity getProcessGroupDetails(String nifiUrl, String processGroupUrl) throws ClientProtocolException, IOException {
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		HttpGet httpGet = new HttpGet(nifiUrl + processGroupUrl);
		HttpResponse response = httpClient.execute(httpGet);
		HttpEntity respEntity = response.getEntity();
		return respEntity;

	}
	
	
	@SuppressWarnings("rawtypes")
	private  JSONObject getControllerObject(String content) throws ClientProtocolException, IOException, org.json.simple.parser.ParseException {
		
		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONArray jsonArray = (JSONArray) jsonObject.get("controllerServices");
		Iterator i = jsonArray.iterator();
		while(i.hasNext()) {
			JSONObject controllerObject=(JSONObject) i.next();
			JSONObject controllerComponent = (JSONObject) controllerObject.get("component");
			String name= (String) controllerComponent.get("name");
			if(!(name.contains("metadata"))) {
				System.out.println(name);
				return controllerObject;
			}
			
		}
		
		
		return null;


	}
	
	private  String getControllerInfo(JSONObject controllerJsonObject) throws Exception{
		
		
		String controllerId="";
		String controllerClientId="";
		String controllerVersion="";
		String controllerStatus="";
	
		controllerId=String.valueOf(controllerJsonObject.get("id"));
		JSONObject controllerRevision = (JSONObject) controllerJsonObject.get("revision");
		JSONObject controllerComponent=(JSONObject) controllerJsonObject.get("component");
		controllerClientId=String.valueOf(controllerRevision.get("clientId"));
		controllerVersion=String.valueOf(controllerRevision.get("version"));
		controllerStatus=String.valueOf(controllerComponent.get("state"));
		return controllerId+","+controllerClientId+","+controllerVersion+","+controllerStatus;
		
	}
	
	private   String processorFree(JSONObject controllerJsonObject)
			throws ParseException, IOException, org.json.simple.parser.ParseException {
		
		int activeThreadCount=0;
		StringBuffer refComponents=new StringBuffer();
		JSONObject controllerComponent=(JSONObject)controllerJsonObject.get("component");
		JSONArray arrayOfReferencingComponents=(JSONArray)controllerComponent.get("referencingComponents");
		for(int i=0;i<arrayOfReferencingComponents.size();i++) {
			JSONObject refComp=(JSONObject)arrayOfReferencingComponents.get(i);
			String refCompId=String.valueOf(refComp.get("id"));
			JSONObject refCompRevision=(JSONObject)refComp.get("revision");
			String refCompVersion=String.valueOf(refCompRevision.get("version"));
			refComponents.append(refCompId+"~"+refCompVersion+",");
			JSONObject refCompComponent=(JSONObject)refComp.get("component");
			if(!String.valueOf(refCompComponent.get("activeThreadCount")).equals("0")) {
				System.out.println("active thread found");
				activeThreadCount++;
			}else {
				System.out.println("No Active thread for processor "+refCompId);
			}
			
		}
		refComponents.setLength(refComponents.length()-1);
		if(activeThreadCount==0) {
			return refComponents.toString();
		}
		else {
			return "Not free";
		}

	}

	
	
	private HttpEntity getControllerServiceDetails(String nifiUrl, String controllerServiceUrl) throws ClientProtocolException, IOException {
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		HttpGet httpGet = new HttpGet(nifiUrl + controllerServiceUrl);
		HttpResponse response = httpClient.execute(httpGet);
		HttpEntity respEntity = response.getEntity();
		return respEntity;

	}
	
	
	private  String getClientId(String content) throws Exception {

		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		System.out.println("----->"+jsonObject.toJSONString());
		JSONObject jsonVersionObject = (JSONObject) jsonObject.get("revision");
		String version = jsonVersionObject.get("version").toString();
		Object clientID = jsonVersionObject.get("clientId");
		
		if(clientID == null) {
			clientID=UUID.randomUUID().toString();
		}
		
		return clientID + "," + version;

	}
	
	
	

	
	
	
	private  void stopReferencingComponents(String processorInfo,String clientId) throws Exception {
		
		for (String processor : processorInfo.split(",")) {
			String processorId = processor.split("~")[0];
			String processorVersion = processor.split("~")[1];
			stopProcessor(processorId, processorVersion, clientId);
		}
	}

	
	private  void stopProcessor(String id,String version, String clientId) throws Exception {
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		

		StringEntity input = new StringEntity(NifiConstants.STOPPROCESSOR.replace("${id}", id)
					.replace("${version}", version).replace("${clientId}", clientId));

			input.setContentType("application/json;charset=UTF-8");
			HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + NifiConstants.PROCESSORURL.replace("${id}", id));

			putRequest.setEntity(input);
			CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
			if (httpResponse.getStatusLine().getStatusCode() != 200) {
				System.out.println(EntityUtils.toString(httpResponse.getEntity()));
				throw new Exception("exception occured while stoping processor");
			}
			else {
				System.out.println("processor with id "+id+" stopped ");
			}

		
	}
	
	private  void disableController(String controllerId) throws Exception {
		
		HttpEntity respEntity=null;
		String clientId="";
		String controllerVersion="";
		
		
		respEntity=getControllerServiceDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
		if (respEntity != null) {
			String content = EntityUtils.toString(respEntity);
			String clientIdVersion=getClientId(content);
			clientId=clientIdVersion.split(",")[0];
			controllerVersion=clientIdVersion.split(",")[1];
			
		}
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		
		String DISABLECOMMAND=NifiConstants.DISABLECONTROLLERCOMMAND.replace("${clientid}", clientId).replace("${version}", controllerVersion).replace("${id}",controllerId);
		System.out.println("command is "+DISABLECOMMAND);
		StringEntity input = new StringEntity(
				DISABLECOMMAND);
		input.setContentType("application/json;charset=UTF-8");
		System.out.println(input.toString());
		HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + "nifi-api/controller-services/"+controllerId );
		putRequest.setEntity(input);

		CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
		if (httpResponse.getStatusLine().getStatusCode() != 200) {
			System.out.println(httpResponse.getStatusLine().toString());
			System.out.println(EntityUtils.toString(httpResponse.getEntity()));
			throw new Exception("exception occured while disabling controller");
		}
		else {
			System.out.println("controller disabled");
		}

	}
		
	@SuppressWarnings("static-access")
	private void updateController(String connUrl,String uname,byte[] encrypted_key,byte[] encrypted_password,String controllerId) throws Exception {

		HttpEntity respEntity=null;
		String state="";
		String clientId="";
		String controllerVersion="";
		String password=iExtract.decyptPassword(encrypted_key, encrypted_password);
		System.out.println("decrypted password is "+password);
		
		do {
			Thread.currentThread().sleep(5000);
			respEntity=getControllerServiceDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
			if (respEntity != null) {
				String content = EntityUtils.toString(respEntity);
				state=getControllerState(content);
				if(state.equalsIgnoreCase("DISABLED")) {
					String controllerInfo=getClientId(content);
					clientId=controllerInfo.split(",")[0];
					controllerVersion=controllerInfo.split(",")[1];
					
				}
		}
		
	}while(!state.equalsIgnoreCase("DISABLED"));
		System.out.println("controller is now disabled");
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		StringEntity input = new StringEntity(NifiConstants.UPDATEDBCONNECTIONPOOL.replace("${clientId}", clientId)
				.replace("${ver}", controllerVersion).replace("${contId}", controllerId)
				.replace("${conUrl}", connUrl).replace("${user}", uname)
				.replace("${pasword}", password));

		input.setContentType("application/json;charset=UTF-8");
		HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + "nifi-api/controller-services/"+controllerId);

		putRequest.setEntity(input);
		CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
		if (httpResponse.getStatusLine().getStatusCode() != 200) {
			System.out.println(EntityUtils.toString(httpResponse.getEntity()));
			throw new Exception("exception occured while updating controller");
		}

	}
	
	
	private  String getControllerState(String content) throws org.json.simple.parser.ParseException {
		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONObject controllerComponent = (JSONObject) jsonObject.get("component");
		String state = controllerComponent.get("state").toString();
		return state;
	}
	
	private  void enableController(String controllerId) throws Exception{
		
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		HttpEntity respEntity=null;
		String controllerInfo="";
		String clientId="";
		String controllerVersion="";
		respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
		if (respEntity != null) {
			String content = EntityUtils.toString(respEntity);
			controllerInfo=getClientId(content);
			clientId=controllerInfo.split(",")[0];
			controllerVersion=controllerInfo.split(",")[1];
	}
		
			StringEntity input = new StringEntity(NifiConstants.ENABLEDBCONNECTIONPOOL.replace("${clientId}", clientId)
					.replace("${ver}", controllerVersion).replace("${contId}", controllerId));
			input.setContentType("application/json;charset=UTF-8");
			HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + "nifi-api/controller-services/"+controllerId);

			putRequest.setEntity(input);
			CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
			if (httpResponse.getStatusLine().getStatusCode() != 200) {
				System.out.println(EntityUtils.toString(httpResponse.getEntity()));
				throw new Exception("exception occured while enabling controller");
			}
			else {
				System.out.println("controller Enabling started");
			}
		}
		
	
	
	

	
	@SuppressWarnings("static-access")
	private  void startReferencingComponents(String controllerId,String processGroupUrl)
			throws Exception {
		HttpEntity respEntity=null;
		String state="";
		String clientId="";
		
		do {
			Thread.currentThread().sleep(5000);
			respEntity=getControllerServiceDetails(NifiConstants.NIFIURL, "nifi-api/controller-services/"+controllerId);
			if (respEntity != null) {
				String content = EntityUtils.toString(respEntity);
				state=getControllerState(content);
			
		}
		
	}while(!state.equalsIgnoreCase("ENABLED"));
		System.out.println("controller is now enabled");
		respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
		if (respEntity != null) {
			String content = EntityUtils.toString(respEntity);
			JSONObject controllerObj=getControllerObject(content);
			StringBuffer refComponents=new StringBuffer();
			JSONObject controllerComponent=(JSONObject)controllerObj.get("component");
			JSONArray arrayOfReferencingComponents=(JSONArray)controllerComponent.get("referencingComponents");
			for(int i=0;i<arrayOfReferencingComponents.size();i++) {
				JSONObject refComp=(JSONObject)arrayOfReferencingComponents.get(i);
				String refCompId=String.valueOf(refComp.get("id"));
				JSONObject refCompRevision=(JSONObject)refComp.get("revision");
				String refCompVersion=String.valueOf(refCompRevision.get("version"));
				refComponents.append(refCompId+"~"+refCompVersion+",");
		}
		refComponents.setLength(refComponents.length()-1);
		for(String processor:refComponents.toString().split(",")) {
			String processorId=processor.split("~")[0];
			String processorVersion=processor.split("~")[1];
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			StringEntity input = new StringEntity(NifiConstants.STARTPROCESSOR.replace("${id}", processorId)
					.replace("${version}", processorVersion).replace("${clientId}", clientId));

			input.setContentType("application/json;charset=UTF-8");
			HttpPut putRequest = new HttpPut(NifiConstants.NIFIURL + NifiConstants.PROCESSORURL.replace("${id}", processorId));

			putRequest.setEntity(input);
			CloseableHttpResponse httpResponse = httpClient.execute(putRequest);
			if (httpResponse.getStatusLine().getStatusCode() != 200) {
				System.out.println(EntityUtils.toString(httpResponse.getEntity()));
				throw new Exception("exception occured while starting processor");
			}

		}

	}
	}
	
		
	


	@SuppressWarnings("unchecked")
	private  JSONArray createJsonObject(int index,RealTimeExtractDto rtExtractDto,String conn_string,String path,String date,Long runId) throws Exception {

		JSONArray arr = new JSONArray();
		HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();
		
		for(TableMetadataDto tableMetadata: rtExtractDto.getTableInfoDto().getTableMetadataArr()) {
			StringBuffer gcsTarget=new StringBuffer();
			StringBuffer hdfsTarget=new StringBuffer();
			JSONObject json=new JSONObject();
			StringBuffer columnsWithQuotes=new StringBuffer();
			String[] columns=tableMetadata.getColumns().split(",");
			for(String column :columns) {
				column =new StringBuilder()
		        .append('\'')
		        .append(column)
		        .append('\'')
		        .append(',')
		        .toString();
				columnsWithQuotes.append(column);
			}
			columnsWithQuotes.setLength((columnsWithQuotes.length()-1));
			
			json.put("table_name", tableMetadata.getTable_name());
			if(!(tableMetadata.getColumns().equalsIgnoreCase("all"))) {
				json.put("columns", tableMetadata.getColumns());
				json.put("columns_where_clause","where upper(table_name)='"+tableMetadata.getTable_name().split("\\.")[1].toUpperCase()
						+"' and upper(owner)='"+tableMetadata.getTable_name().split("\\.")[0].toUpperCase()
						+ "' and upper(column_name) in("+columnsWithQuotes.toString().toUpperCase()+")");
			}else {
				json.put("columns_where_clause","where upper(table_name)='"+tableMetadata.getTable_name().split("\\.")[1].toUpperCase()
						+"' and upper(owner)='"+tableMetadata.getTable_name().split("\\.")[0].toUpperCase()+"'");
			}
			
			json.put("where_clause", tableMetadata.getWhere_clause());
			if(tableMetadata.getFetch_type().equalsIgnoreCase("INCR")) {
				json.put("incremental_column", tableMetadata.getIncr_col());
			}
			json.put("project_sequence", rtExtractDto.getFeedDto().getProject_sequence());
			json.put("process_group", index);
			json.put("country_code", rtExtractDto.getFeedDto().getCountry_code());
			json.put("feed_id",Integer.toString(rtExtractDto.getFeedDto().getFeed_id()));
			json.put("feed_name", rtExtractDto.getFeedDto().getFeed_name());
			json.put("date", date);
			json.put("run_id", runId);
			json.put("path", path);
			for(TargetDto targetDto :rtExtractDto.getTargetArr()) {
				if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
					gcsTarget.append(targetDto.getTarget_project()+"~");
					gcsTarget.append(targetDto.getService_account()+"~");
					gcsTarget.append(targetDto.getTarget_bucket()+",");
				}
				if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
					
					String password=iExtract.decyptPassword(targetDto.getEncrypted_key(), targetDto.getEncrypted_password());
					
					hdfsTarget.append(targetDto.getTarget_knox_url()+"~");
					hdfsTarget.append(targetDto.getTarget_user()+"~");
					hdfsTarget.append(password+"~");
					hdfsTarget.append(targetDto.getTarget_hdfs_path()+",");
				}
				
				
			}
			if(gcsTarget.length()>1) {
				gcsTarget.setLength(gcsTarget.length()-1);
				json.put("gcsTarget", gcsTarget.toString());
				System.out.println("gcs target is "+gcsTarget.toString());
			}
			if(hdfsTarget.length()>1) {
				hdfsTarget.setLength(hdfsTarget.length()-1);
				json.put("hdfsTarget", hdfsTarget.toString());
				System.out.println("hdfs target is "+hdfsTarget.toString());
			}

			map.put(tableMetadata.getTable_name()+"_obj", json);
			arr.add(map.get(tableMetadata.getTable_name()+"_obj"));
			
		}
		
		return arr;

	}
	
	@SuppressWarnings("unchecked")
	private  JSONArray createUnixJsonObject(RealTimeExtractDto rtExtractDto,String path,int index,String date,Long runId) throws Exception {

		JSONArray arr = new JSONArray();
		StringBuffer gcsTarget=new StringBuffer();
		StringBuffer hdfsTarget=new StringBuffer();
		for(FileMetadataDto file: rtExtractDto.getFileInfoDto().getFileMetadataArr()) {
			JSONObject json=new JSONObject();
			json.put("process_group", index);
			json.put("project_sequence", rtExtractDto.getFeedDto().getProject_sequence());
			json.put("feed_id", rtExtractDto.getFeedDto().getFeed_id());
			json.put("file_sequence", file.getFile_sequence());
			json.put("file_name", file.getFile_name());
			json.put("header_count", file.getHeader_count());
			json.put("avro_conv_flg", file.getAvro_conv_flag());
			json.put("field_list", file.getField_list());
			json.put("date", date);
			json.put("file_type", file.getFile_type());
			json.put("file_delimiter", file.getFile_delimiter());
			json.put("feed_name", rtExtractDto.getFeedDto().getFeed_name());
			json.put("country_code", rtExtractDto.getFeedDto().getCountry_code());
			json.put("file_path", rtExtractDto.getFeedDto().getFilePath());
			json.put("run_id", runId);
			json.put("date", date);
			for(TargetDto targetDto :rtExtractDto.getTargetArr()) {
				if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
					gcsTarget.append(targetDto.getTarget_project()+"~");
					gcsTarget.append(targetDto.getService_account()+"~");
					gcsTarget.append(targetDto.getTarget_bucket()+",");
				}
				if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
					
					String password=iExtract.decyptPassword(targetDto.getEncrypted_key(), targetDto.getEncrypted_password());
					
					hdfsTarget.append(targetDto.getTarget_knox_url()+"~");
					hdfsTarget.append(targetDto.getTarget_user()+"~");
					hdfsTarget.append(password+"~");
					hdfsTarget.append(targetDto.getTarget_hdfs_path()+",");
				}
				
				
			}
			if(gcsTarget.length()>1) {
				gcsTarget.setLength(gcsTarget.length()-1);
				json.put("gcsTarget", gcsTarget.toString());
			}
			if(hdfsTarget.length()>1) {
				hdfsTarget.setLength(hdfsTarget.length()-1);
				json.put("hdfsTarget", hdfsTarget.toString());
			}
			
			arr.add(json);
			
		}
	
		return arr;
	}
	
	@SuppressWarnings("unchecked")	
	private JSONArray createHadoopJsonObject(RealTimeExtractDto rtExtractDto, String date, Long runId) {
		
		JSONArray arr = new JSONArray();
		for(String filePath: rtExtractDto.getHdfsInfoDto().getHdfsPath()) {
			JSONObject json=new JSONObject();
			json.put("file_path", filePath);
			json.put("knox_url", "https://"+rtExtractDto.getConnDto().getHostName()+":"+rtExtractDto.getConnDto().getPort());
			json.put("knox_user", rtExtractDto.getConnDto().getUserName());
			json.put("knox_password", rtExtractDto.getConnDto().getPassword());
			int i=1;
			for(TargetDto tarDto: rtExtractDto.getTargetArr()) {
				if(tarDto.getTarget_type().equalsIgnoreCase("gcs")) {
					json.put("target_type"+Integer.toString(i), "gcs");
					json.put("bucket"+Integer.toString(i), tarDto.getTarget_bucket());
				}
				if(tarDto.getTarget_type().equalsIgnoreCase("hdfs")) {
					json.put("target_type"+Integer.toString(i), "hdfs");
					json.put("knox_url"+Integer.toString(i), tarDto.getTarget_knox_url());
					json.put("knox_user"+Integer.toString(i), tarDto.getTarget_user());
					json.put("knox_password"+Integer.toString(i), tarDto.getTarget_password());
					json.put("hdfs_path"+Integer.toString(i), tarDto.getTarget_hdfs_path());
				}
				if(tarDto.getTarget_type().equalsIgnoreCase("unix")) {
					
					json.put("target_unix_path"+Integer.toString(i), tarDto.getFull_path());
					
				}
				
				
				i++;
			}
			arr.add(json);
			
		}
		return arr;
	}

	
	@SuppressWarnings("rawtypes")
	private  void invokeNifiFull(JSONArray arr,String  listenHttpUrl) throws UnsupportedOperationException, Exception {
		
	
			
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			Iterator it = arr.iterator();
			while (it.hasNext()) {
				JSONObject json=(JSONObject) it.next();
				HttpPost postRequest=new HttpPost(listenHttpUrl);
				StringEntity input = new StringEntity(json.toString());
				postRequest.setEntity(input); 
				HttpResponse response = httpClient.execute(postRequest);
				if(response.getStatusLine().getStatusCode()!=200) {
						System.out.println("Nifi listener problem"+response.getStatusLine().getStatusCode());
						throw new Exception("Nifi Not Running Or Some Problem In Sending HTTP Request"+response.getEntity().getContent().toString());
				}
				else {
					System.out.println("Nifi Triggered");
				}
			}
			
		}
	
	
	
	
/*public  void createSystemDetailsFile(RealTimeExtractDto rtExtractDto,Long runId,String date) throws IOException, JSchException {

	StringBuffer bucketNames = new StringBuffer();
	StringBuffer hdfsPath=new StringBuffer();
		
		for(TargetDto targetDto:rtExtractDto.getTargetArr()) {
			if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
				bucketNames.append(targetDto.getTarget_bucket()+",");
			}if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
				hdfsPath.append(targetDto.getTarget_hdfs_path()+",");
			}
					}

		
		
		StringBuffer stringBufferGcp = new StringBuffer();
		StringBuffer dataPathGcp=new StringBuffer();
		StringBuffer dataPathHdfs=new StringBuffer();
		
		if(!(bucketNames.toString().isEmpty()||bucketNames.toString().equals(null))){
			bucketNames.setLength(bucketNames.length()-1);
			for(String bucket:bucketNames.toString().split(",")) {
				dataPathGcp.append("gs://"+bucket+"/"+rtExtractDto.getSrsSysDto().getCountry_code()+"/"+rtExtractDto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/DATA"+",");
				
			}
		}
		if(!(hdfsPath.toString().equals(null)||hdfsPath.toString().isEmpty())) {
			hdfsPath.setLength(hdfsPath.length()-1);
			for(String path:hdfsPath.toString().split(",")) {
				dataPathHdfs.append(path+"/"+rtExtractDto.getSrsSysDto().getCountry_code()+"/"+rtExtractDto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/DATA"+",");
				
			}
		}
		
		JSch obj_JSch = new JSch();
		//obj_JSch.addIdentity("/home/birajrath2008/.ssh/id_rsa");
	    Session obj_Session = null;
	    try {
			obj_Session = obj_JSch.getSession("extraction_user", NifiConstants.NIFINSTANCEIP);
			obj_Session.setPort(22);
			obj_Session.setPassword("Infy@123");
			Properties obj_Properties = new Properties();
			obj_Properties.put("StrictHostKeyChecking", "no");
			obj_Session.setConfig(obj_Properties);
			obj_Session.connect();
			Channel obj_Channel = obj_Session.openChannel("sftp");
			obj_Channel.connect();
			ChannelSftp obj_SFTPChannel = (ChannelSftp) obj_Channel;
			
			if(!(bucketNames.toString().isEmpty()||bucketNames.toString().equals(null))){
				
				dataPathGcp.setLength(dataPathGcp.length()-1);
				stringBufferGcp.append("src_sys_name,country_code,src_sys_desc,bucket,path,project,c1,c2,metadata_status");
				
				stringBufferGcp.append(rtExtractDto.getSrsSysDto().getSrc_unique_name()+","+","+rtExtractDto.getConnDto().getConn_type() + " FEED"+","+bucketNames.toString()+","+dataPathGcp+","+GenericConstants.PROJECTID+","
						+","+","+"MC");
				
				InputStream obj_InputStream = new ByteArrayInputStream(stringBufferGcp.toString().getBytes());
				String homePath="/home/google/";
				obj_SFTPChannel.cd(homePath);
				String path=rtExtractDto.getSrsSysDto().getCountry_code()+"/"+rtExtractDto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/metadata";
				System.out.println("path is "+path);
				String[] folders = path.split( "/" );
				for ( String folder : folders ) {
				    if ( folder.length() > 0 ) {
				        try {
				        	obj_SFTPChannel.cd(folder);
				        	
				        }
				        catch ( SftpException e ) {
				        	obj_SFTPChannel.mkdir(folder);
				        	obj_SFTPChannel.cd(folder);
				        	
				        }
				    }
				}
				
				obj_SFTPChannel.put(obj_InputStream,"/home/google/"+ path+"/" + "mstr_src_sys_dtls.csv" );
				obj_SFTPChannel.exit();
				obj_InputStream.close();
				obj_Channel.disconnect();
				obj_Session.disconnect();
				
	       
			}
	    }catch ( Exception e ) {
			e.printStackTrace();
		}
		}
		
		

		
	

public  void createFileDetailsFile(RealTimeExtractDto rtExtractDto,Long runId,String date) throws IOException, JSchException, SftpException  {
	
	
	
	
	StringBuffer filebuffer = new StringBuffer();
	filebuffer.append("\n");
	for(TableMetadataDto tbl:rtExtractDto.getTableInfoDto().getTableMetadataArr()) {
		if(tbl.getFetch_type().equalsIgnoreCase("full")) {
			filebuffer.append(rtExtractDto.getSrsSysDto().getSrc_unique_name()+","+tbl.getTable_name()+","+tbl.getTable_name()+","+rtExtractDto.getSrsSysDto().getSrc_sys_id()+"_samplefile"+","+"A"+","+""+","+tbl.getTable_name()+","+","+"0"+","+"0"+","+"0"+","+"0"+","+"SQL"+","+"F"+"\n");
		}
		else {
			filebuffer.append(rtExtractDto.getSrsSysDto().getSrc_unique_name()+","+tbl.getTable_name()+","+tbl.getTable_name()+","+rtExtractDto.getSrsSysDto().getSrc_sys_id()+"_samplefile"+","+"A"+","+""+","+tbl.getTable_name()+","+","+"0"+","+"0"+","+"0"+","+"0"+","+"SQL"+","+"I"+"\n");
		}
		
		}
	JSch obj_JSch = new JSch();
	//obj_JSch.addIdentity("/home/birajrath2008/.ssh/id_rsa");
    Session obj_Session = null;
	try {
			obj_Session = obj_JSch.getSession("extraction_user", NifiConstants.NIFINSTANCEIP);
			obj_Session.setPort(22);
			obj_Session.setPassword("Infy@123");
			Properties obj_Properties = new Properties();
			obj_Properties.put("StrictHostKeyChecking", "no");
			obj_Session.setConfig(obj_Properties);
			obj_Session.connect();
			Channel obj_Channel = obj_Session.openChannel("sftp");
			obj_Channel.connect();
			ChannelSftp obj_SFTPChannel = (ChannelSftp) obj_Channel;
			InputStream obj_InputStream = new ByteArrayInputStream(filebuffer.toString().getBytes());
			String homePath="/home/google/";
			obj_SFTPChannel.cd(homePath);
			String path=rtExtractDto.getSrsSysDto().getCountry_code()+"/"+rtExtractDto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/metadata";
			System.out.println("path is "+path);
			String[] folders = path.split( "/" );
			for ( String folder : folders ) {
			    if ( folder.length() > 0 ) {
			        try {
			        	obj_SFTPChannel.cd(folder);
			        	System.out.println("inside "+folder);
			        }
			        catch ( SftpException e ) {
			        	obj_SFTPChannel.mkdir(folder);
			        	obj_SFTPChannel.cd(folder);
			        	System.out.println(folder+" created");
			        }
			    }
			}

			obj_SFTPChannel.put(obj_InputStream,"/home/google/"+ path+"/" + "mstr_file_dtls.csv" );
			obj_SFTPChannel.exit();
			obj_InputStream.close();
			obj_Channel.disconnect();
			obj_Session.disconnect();
			
	       
			}catch ( Exception e ) {
    		e.printStackTrace();
    	}
		



}*/

	
}
