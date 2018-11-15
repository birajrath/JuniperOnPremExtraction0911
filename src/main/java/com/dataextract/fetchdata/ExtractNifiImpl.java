package com.dataextract.fetchdata;



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
import com.dataextract.constants.GenericConstants;
import com.dataextract.constants.NifiConstants;
import com.dataextract.dao.IExtractionDAO;
import com.dataextract.dto.FileMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.repositories.DataExtractRepositories;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

@Repository
public class ExtractNifiImpl implements IExtract {


	@Autowired
	private DataExtractRepositories dataExtractRepositories;

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
	
		
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("MSSQL")) {
			
			for(int i =0;i<100;i++) {
				NifiConstants constants=new NifiConstants(); 
				index=getRandomNumberInRange(1, NifiConstants.NOOFMSSQLPROCESSORS);
				String processor="MSSQLPROCESSGROUPURL"+index;
				processGroupUrl = String.valueOf(NifiConstants.class.getDeclaredField(processor).get(constants));
				String listener="MSSQLLISTENER"+index;
				listenHttpUrl= String.valueOf(NifiConstants.class.getDeclaredField(listener).get(constants));
				System.out.println("process group url is "+processGroupUrl);
				respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
				if (respEntity != null) {
					String content = EntityUtils.toString(respEntity);
					System.out.println(content);
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
							
							break;
						}
					}
				}
			}
		}
		
		 if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE")) {
			
			for(int i =0;i<100;i++) {
				NifiConstants constants=new NifiConstants();
				
				
				index=getRandomNumberInRange(1, NifiConstants.NOOFORACLEPROCESSORS);
				String varName="ORACLEPROCESSGROUPURL"+index;
				processGroupUrl = String.valueOf(NifiConstants.class.getDeclaredField(varName).get(constants));
				
				listenHttpUrl= NifiConstants.ORACLELISTENER1;
				System.out.println("process group url is "+processGroupUrl);
				respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
				if (respEntity != null) {
					String content = EntityUtils.toString(respEntity);
					System.out.println(content);
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
							processGroupIndex=index;
							break;
						}
					}
				}
			}
		}
		
		 if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("TERADATA")) {
				
				for(int i =0;i<100;i++) {
					NifiConstants constants=new NifiConstants();
					index=getRandomNumberInRange(1, NifiConstants.NOOFORACLEPROCESSORS);
					String varName="ORACLEPROCESSGROUPURL"+index;
					processGroupUrl = String.valueOf(NifiConstants.class.getDeclaredField(varName).get(constants));
					String listener="ORACLELISTENER"+index;
					listenHttpUrl= String.valueOf(NifiConstants.class.getDeclaredField(listener).get(constants));
					System.out.println("process group url is "+processGroupUrl);
					respEntity=getProcessGroupDetails(NifiConstants.NIFIURL, processGroupUrl);
					if (respEntity != null) {
						String content = EntityUtils.toString(respEntity);
						System.out.println(content);
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
								break;
							}
						}
					}
				}
			}
		 
		
		stopReferencingComponents(processorInfo, clientId);
		disableController(controllerId);
		updateController(conn_string , rtExtractDto.getConnDto().getUserName(),  rtExtractDto.getConnDto().getPassword(), controllerId);
		enableController(controllerId);
		startReferencingComponents(controllerId,processGroupUrl);
		JSONArray arr=createJsonObject(index,rtExtractDto,conn_string,date, runId);
		invokeNifiFull(arr,listenHttpUrl);
		String updateStatus=dataExtractRepositories.updateNifiProcessgroupDetails(rtExtractDto, date, runId.toString(), processGroupIndex);
		if(!(updateStatus.equalsIgnoreCase("success"))) {
			System.out.println("update status is "+updateStatus);
			return updateStatus;
		}
		else {
			
			try {
				createSystemDetailsFile(rtExtractDto,runId,date);
				createFileDetailsFile(rtExtractDto,runId,date);
				}catch (IOException e) {
					// TODO Auto-generated catch block
					 return e.getMessage();
					 
				}
			return "success";
		}
			
	
	}
	
	@Override
	public String callNifiUnixRealTime(RealTimeExtractDto rtExtractDto, String date, Long runId) throws UnsupportedOperationException, Exception {
		
		String listener=NifiConstants.UNIXLISTENER1;
		JSONArray jsonArr=createUnixJsonObject(rtExtractDto,date, runId);
		invokeNifiFull(jsonArr,listener);
		return "success";
		
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
	
	
	private  JSONObject getControllerObject(String content) throws ClientProtocolException, IOException, org.json.simple.parser.ParseException {
		
		JSONObject jsonObject = (JSONObject) new JSONParser().parse(content);
		JSONArray jsonArray = (JSONArray) jsonObject.get("controllerServices");
		JSONObject controllerJsonObject = (JSONObject) jsonArray.get(0);
		return controllerJsonObject;

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
	private void updateController(String connUrl,String uname, String pwd,String controllerId) throws Exception {

		HttpEntity respEntity=null;
		String state="";
		String clientId="";
		String controllerVersion="";
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
				.replace("${pasword}", pwd));

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
	private  JSONArray createJsonObject(int index,RealTimeExtractDto rtExtractDto,String conn_string,String date,Long runId) {

		JSONArray arr = new JSONArray();
		HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();
		StringBuffer tableList=new StringBuffer();
		StringBuffer tableListWithQuotes=new StringBuffer();
		for(Map<String,String> table: rtExtractDto.getTableInfoDto().getTableInfo()) {
			JSONObject json=new JSONObject();
			String[] temp=table.get("table_name").split("\\.");
			if(temp.length>1) {
				
				tableList.append(temp[1]+",");
			}
			else {
				
				tableList.append(table.get("table_name")+",");
			}
			json.put("table_name", table.get("table_name"));
			json.put("columns", table.get("columns"));
			json.put("where_clause", table.get("where_clause"));
			if(!(table.get("fetch_type").equalsIgnoreCase("FULL"))) {
				json.put("incremental_column", table.get("incr_col"));
			}
			
			json.put("process_group", index);
			json.put("country_code", rtExtractDto.getSrsSysDto().getCountry_code());
			json.put("src_sys_id",Integer.toString(rtExtractDto.getSrsSysDto().getSrc_sys_id()));
			json.put("src_unique_name", rtExtractDto.getSrsSysDto().getSrc_unique_name());
			json.put("date", date);
			json.put("run_id", runId);
			json.put("path", rtExtractDto.getSrsSysDto().getCountry_code()+"/"+ rtExtractDto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/");
			json.put("metadata_tables", "00000");
			int i=1;
			for(TargetDto targetDto :rtExtractDto.getTargetArr()) {
				if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
					json.put("target_type"+Integer.toString(i), "gcs");
					json.put("bucket"+Integer.toString(i), targetDto.getTarget_bucket());
				}
				if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
					json.put("target_type"+Integer.toString(i), "hdfs");
					json.put("knox_url"+Integer.toString(i), targetDto.getTarget_knox_url());
					json.put("knox_user"+Integer.toString(i), targetDto.getTarget_user());
					json.put("knox_password"+Integer.toString(i), targetDto.getTarget_password());
					json.put("hdfs_path"+Integer.toString(i), targetDto.getTarget_hdfs_path());
				}
				
				i++;
			}
			
			
			
			
			
			map.put(table.get("table_name")+"_obj", json);
			arr.add(map.get(table.get("table_name")+"_obj"));
			
		}
		tableList.setLength(tableList.length()-1);
		String[] tempList=tableList.toString().split(",");
		for(String table :tempList) {
			System.out.println("table------"+table);
			//System.out.println(table.split("\\.")[1]);
			table =new StringBuilder()
	        .append('\'')
	        .append(table)
	        .append('\'')
	        .append(',')
	        .toString();
			tableListWithQuotes.append(table);
		}
		tableListWithQuotes.setLength((tableListWithQuotes.length()-1));
		JSONObject json=new JSONObject();
		json.put("metadata_tables",tableListWithQuotes.toString());
		json.put("path", rtExtractDto.getSrsSysDto().getCountry_code()+"/"+ rtExtractDto.getSrsSysDto().getSrc_unique_name()+"/"+date+"/"+runId+"/");
		int i=1;
		for(TargetDto targetDto :rtExtractDto.getTargetArr()) {
			if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
				json.put("target_type"+Integer.toString(i), "gcs");
				json.put("bucket"+Integer.toString(i), targetDto.getTarget_bucket());
			}
			if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
				json.put("target_type"+Integer.toString(i), "hdfs");
				json.put("knox_url"+Integer.toString(i), targetDto.getTarget_knox_url());
				json.put("knox_user"+Integer.toString(i), targetDto.getTarget_user());
				json.put("knox_password"+Integer.toString(i), targetDto.getTarget_password());
				json.put("hdfs_path"+Integer.toString(i), targetDto.getTarget_hdfs_path());
			}
			
			i++;
		}
		json.put("process_group", index);
		json.put("src_unique_name", rtExtractDto.getSrsSysDto().getSrc_unique_name());
		map.put("metadata_details", json);
		arr.add(map.get("metadata_details"));
		System.out.println("Json Array size------"+arr.size());
		System.out.println("json array"+arr.toString());
	

		return arr;

	}
	
	@SuppressWarnings("unchecked")
	private  JSONArray createUnixJsonObject(RealTimeExtractDto rtExtractDto,String date,Long runId) {

		JSONArray arr = new JSONArray();
		for(FileMetadataDto file: rtExtractDto.getFileInfoDto().getFileMetadataArr()) {
			JSONObject json=new JSONObject();
			json.put("file_name", file.getFile_name());
			json.put("avro_conv_flg", file.getAvro_conv_flag());
			json.put("field_list", file.getField_list());
			json.put("file_type", file.getFile_type());
			json.put("file_delimiter", file.getFile_delimiter());
			json.put("src_unique_name", rtExtractDto.getSrsSysDto().getSrc_unique_name());
			json.put("country_code", rtExtractDto.getSrsSysDto().getCountry_code());
			json.put("file_path", rtExtractDto.getSrsSysDto().getFilePath());
			json.put("run_id", runId);
			json.put("date", date);
			int i=1;
			for(TargetDto targetDto :rtExtractDto.getTargetArr()) {
				if(targetDto.getTarget_type().equalsIgnoreCase("gcs")) {
					json.put("target_type"+Integer.toString(i), "gcs");
					json.put("bucket"+Integer.toString(i), targetDto.getTarget_bucket());
				}
				if(targetDto.getTarget_type().equalsIgnoreCase("hdfs")) {
					json.put("target_type"+Integer.toString(i), "hdfs");
					json.put("knox_url"+Integer.toString(i), targetDto.getTarget_knox_url());
					json.put("knox_user"+Integer.toString(i), targetDto.getTarget_user());
					json.put("knox_password"+Integer.toString(i), targetDto.getTarget_password());
					json.put("hdfs_path"+Integer.toString(i), targetDto.getTarget_hdfs_path());
				}
				
				i++;
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

	
	private  void invokeNifiFull(JSONArray arr,String  listenHttpUrl) throws UnsupportedOperationException, Exception {
		
	
			
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			
			HttpPost postRequest=new HttpPost(listenHttpUrl);
			StringEntity input = new StringEntity(arr.toString());
			postRequest.setEntity(input); 
			HttpResponse response = httpClient.execute(postRequest);
			if(response.getStatusLine().getStatusCode()!=200) {
					System.out.println("Nifi listener problem"+response.getStatusLine().getStatusCode());
					throw new Exception("Nifi Not Running Or Some Problem In Sending HTTP Request"+response.getEntity().getContent().toString());
			}
		}
	
	
	
	
public  void createSystemDetailsFile(RealTimeExtractDto rtExtractDto,Long runId,String date) throws IOException, JSchException {

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
	for(Map<String,String> tbl:rtExtractDto.getTableInfoDto().getTableInfo()) {
		if(tbl.get("fetch_type").equalsIgnoreCase("full")) {
			filebuffer.append(rtExtractDto.getSrsSysDto().getSrc_unique_name()+","+tbl.get("table_name")+","+tbl.get("table_name")+","+rtExtractDto.getSrsSysDto().getSrc_sys_id()+"_samplefile"+","+"A"+","+""+","+tbl.get("table_name")+","+","+"0"+","+"0"+","+"0"+","+"0"+","+"SQL"+","+"F"+"\n");
		}
		else {
			filebuffer.append(rtExtractDto.getSrsSysDto().getSrc_unique_name()+","+tbl.get("table_name")+","+tbl.get("table_name")+","+rtExtractDto.getSrsSysDto().getSrc_sys_id()+"_samplefile"+","+"A"+","+""+","+tbl.get("table_name")+","+","+"0"+","+"0"+","+"0"+","+"0"+","+"SQL"+","+"I"+"\n");
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
		



}

	
}
