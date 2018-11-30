package com.dataextract.constants;

//import org.json.simple.JSONArray;


public class NifiConstants {

	
	public final static  String NIFIURL="http://35.243.211.145:9090/";
	public final static String NIFINSTANCEIP="35.243.211.145";
	
	public final static int NOOFORACLEPROCESSORS=3;
	public final static String ORACLEPROCESSGROUPURL1="nifi-api/flow/process-groups/17239146-0167-1000-ffff-ffffee7f24ab/controller-services";
	public final static String ORACLEPROCESSGROUPURL2="nifi-api/flow/process-groups/330e37fb-132c-16ce-ffff-ffffad8bd4de/controller-services";
	public final static String ORACLEPROCESSGROUPURL3="nifi-api/flow/process-groups/1c7af9d7-0167-1000-ffff-ffff866c37aa/controller-services";																			
	public final static String ORACLELISTENER1="http://35.243.211.145:8084/contentListener";
	
	public static final int NOOFUNIXPROCESSORS =3;
	public final static String UNIXPROCESSGROUPURL1="nifi-api/flow/process-groups/3bfa4378-0167-1000-0000-00000f7e2794/controller-services";
	public static final String UNIXLISTENER1 ="http://35.243.211.145:8085/contentListener";
	

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
