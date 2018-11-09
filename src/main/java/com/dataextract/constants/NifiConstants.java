package com.dataextract.constants;

//import org.json.simple.JSONArray;


public class NifiConstants {

	
	public final static  String NIFIURL="http://35.231.171.195:8080/";
	public final static String NIFINSTANCEIP="35.231.171.195";
	public final static String LISTENHTTPURLSQL="http://35.196.51.88:8082/contentListener";
	public final static String SQLLISTENHTTPURLMETADATA="http://35.227.99.37:8094/contentListener";
	public final static String SQLDBCPCONNECTIONPOOLURL="nifi-api/controller-services/2a02fb61-17d3-3605-a71b-72c76f521cb8";
	public final static String SQLMETADATADBCPCONNECTIONPOOLURL="nifi-api/controller-services/73d53069-646a-1974-e979-3e5fc6df69b5";
	public final static String SQLMETADATADBCPCONNECTIONPOOLID="73d53069-646a-1974-e979-3e5fc6df69b5";
	public final static String SQLDBCPCONNECTIONPOOLID="2a02fb61-17d3-3605-a71b-72c76f521cb8";
	public final static int NOOFMSSQLPROCESSORS=5;
	
	public final static int NOOFORACLEPROCESSORS=5;
	public final static String ORACLEPROCESSGROUPURL1="nifi-api/flow/process-groups/12a63e1d-7ecc-1c23-b926-ca17ddff339b/controller-services";
	public final static String ORACLEPROCESSGROUPURL2="nifi-api/flow/process-groups/12a63e2d-7ecc-1c23-acd8-bb924476f6d3/controller-services";
	public final static String ORACLEPROCESSGROUPURL3="nifi-api/flow/process-groups/cabd6195-0166-1000-1a15-98bb8de9e9d3/controller-services";
	public final static String ORACLEPROCESSGROUPURL4="nifi-api/flow/process-groups/cac31d6a-0166-1000-3e9b-297fa014fc95/controller-services";
	public final static String ORACLEPROCESSGROUPURL5="nifi-api/flow/process-groups/cac71e2e-0166-1000-cdc2-bccf594e76c9/controller-services";
	public final static String ORACLELISTENER1="http://35.231.171.195:8084/contentListener";
	public final static String ORACLELISTENER2="http://35.231.171.195:8085/contentListener";
	public final static String ORACLELISTENER3="http://35.231.171.195:8086/contentListener";
	public final static String ORACLELISTENER4="http://35.231.171.195:8087/contentListener";
	public final static String ORACLELISTENER5="http://35.231.171.195:8088/contentListener";
	
	public final static String UNIXLISTENER1="http://35.231.171.195:8090/contentListener";
	
	public final static String HADOOPLISTENER1="http://35.231.171.195:8091/contentListener";
	
	public final static String DISABLECONTROLLERCOMMAND="{\"revision\":{\"clientId\":\"${clientid}\",\"version\":${version}},\"component\":{\"id\":\"${id}\",\"state\":\"DISABLED\"}}";
	public final static String PROCESSORURL="nifi-api/processors/${id}";
	public final static String STOPPROCESSOR="{\n" + 
			"  \"status\": {\n" + 
			"    \"runStatus\": \"STOPPED\"\n" + 
			"  },\n" + 
			"  \"component\": {\n" + 
			"    \"state\": \"STOPPED\",\n" + 
			"    \"id\": \"${id}\"\n" + 
			"  },\n" + 
			"  \"id\": \"${id}\",\n" + 
			"  \"revision\": {\n" + 
			"    \"version\": ${version},\n" + 
			"    \"clientId\": \"${clientId}\"\n" + 
			"  }\n" + 
			"}";
	public final static String STARTPROCESSOR="{\n" + 
			"  \"status\": {\n" + 
			"    \"runStatus\": \"RUNNING\"\n" + 
			"  },\n" + 
			"  \"component\": {\n" + 
			"    \"state\": \"RUNNING\",\n" + 
			"    \"id\": \"${id}\"\n" + 
			"  },\n" + 
			"  \"id\": \"${id}\",\n" + 
			"  \"revision\": {\n" + 
			"    \"version\": ${version},\n" + 
			"    \"clientId\": \"${clientId}\"\n" + 
			"  }\n" + 
			"}";
	
	public final static String STOPPROCESSOR2="{\n" + 
			"  \"status\": {\n" + 
			"    \"runStatus\": \"STOPPED\"\n" + 
			"  },\n" + 
			"  \"component\": {\n" + 
			"    \"state\": \"STOPPED\",\n" + 
			"    \"id\": \"${id}\"\n" + 
			"  },\n" + 
			"  \"id\": \"${id}\",\n" + 
			"  \"revision\": {\n" + 
			"    \"version\": ${version},\n" + 
			"	\"clientId\": \"\" \n"+
			"  }\n" + 
			"}";
	
	public final static String UPDATEDBCONNECTIONPOOL="{\"revision\":{\"clientId\":\"${clientId}\",\"version\":${ver}},\"component\":{\"id\":\"${contId}\",\"state\":\"DISABLED\", \"properties\":{\"Database Connection URL\":\"${conUrl}\",\"Database User\":\"${user}\",\"Password\":\"${pasword}\"}}}";
	public final static String ENABLEDBCONNECTIONPOOL="{\"revision\":{\"clientId\":\"${clientId}\",\"version\":${ver}},\"component\":{\"id\":\"${contId}\",\"state\":\"ENABLED\"}}";
	public final static String UPDATEGCSPUTPROCESSOR="{\n" + 
			"  \"status\": {\n" + 
			"    \"runStatus\": \"RUNNING\"\n" + 
			"  },\n" + 
			"  \"component\": {\n" + 
			"    \"state\": \"RUNNING\",\n" + 
			"    \"id\": \"${id}\",\n" + 
			"   \"config\": {\n"+
			"   \"properties\": {\n"+
			"   \"gcs-bucket\": \"${bucket}\",\n"+
			"   \"gcs-key\" : \"${path}\"\n" +
			"  }\n" + 
			"  }\n" +
            "  },\n" +
			"  \"id\": \"${id}\",\n" + 
			"  \"revision\": {\n" + 
			"    \"version\": ${version},\n" + 
			"	\"clientId\": \"\" \n"+
			"  }\n" + 
			"}";
	
	public static String UPDATEGENERATETABLEFETCH="{\n" + 
			"  \"status\": {\n" + 
			"    \"runStatus\": \"RUNNING\"\n" + 
			"  },\n" + 
			"  \"component\": {\n" + 
			"    \"state\": \"RUNNING\",\n" + 
			"    \"id\": \"${id}\",\n" + 
			"   \"config\": {\n"+
			"   \"properties\": {\n"+
			"   \"Maximum-value Columns\": \"${mvc}\",\n"+
			"   \"Table Name\": \"${table}\"\n"+
			//"   \"gcs-key\" : \"${path}\"\n" +
			"  }\n" + 
			"  }\n" +
            "  },\n" +
			"  \"id\": \"${id}\",\n" + 
			"  \"revision\": {\n" + 
			"    \"version\": ${version},\n" + 
			"	\"clientId\": \"${clientid}\"\n" +
			"  }\n" + 
			"}";
	
}
