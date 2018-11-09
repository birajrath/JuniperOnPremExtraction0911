package com.dataextract.fetchdata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.dataextract.dto.RealTimeExtractDto;

@Repository
public class ExtractJavaImpl implements ExtractJava {
	
	public static Logger logger = LoggerFactory.getLogger(ExtractJavaImpl.class);
	public static Connection con; 
	public static Statement st;
	private static ExecutorService pool;
	
	@Override
	public String pullData(RealTimeExtractDto rtDataExtractDto) throws IOException {
		
		
		
			String connectionString="";
			String userName=rtDataExtractDto.getConnDto().getUserName();
			String password=rtDataExtractDto.getConnDto().getPassword();
			String db_type=rtDataExtractDto.getConnDto().getConn_type();
			String src_sys_id=Integer.toString(rtDataExtractDto.getSrsSysDto().getSrc_sys_id());
			String src_unique_name=rtDataExtractDto.getSrsSysDto().getSrc_unique_name();
			String targetLocation=rtDataExtractDto.getSrsSysDto().getTarget();
			ArrayList<Map<String, String>> tableInfo= rtDataExtractDto.getTableInfoDto().getTableInfo();
			
			if(rtDataExtractDto.getConnDto().getConn_type().equalsIgnoreCase("MSSQL")){
				
				connectionString= "jdbc:sqlserver://"+ rtDataExtractDto.getConnDto().getHostName()+":" +rtDataExtractDto.getConnDto().getPort()+";DatabaseName="+rtDataExtractDto.getConnDto().getDbName() ;
			}
		    if(rtDataExtractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE"))
			{
		    	connectionString= "jdbc:oracle:thin:@"+ rtDataExtractDto.getConnDto().getHostName()+":" +rtDataExtractDto.getConnDto().getPort()+"/"+rtDataExtractDto.getConnDto().getServiceName() ;
			}
			

			String runId = getRunId();
			pool = Executors.newFixedThreadPool(10);

			ExportMetadataCallable exportMetadataCallable;
			CompletionService<String> completionServiceMetadata = new ExecutorCompletionService<String>(pool);
			
			for(Map<String,String> tblName : tableInfo) {
				exportMetadataCallable = new ExportMetadataCallable(tblName.get("table_name"),connectionString,userName,password,src_sys_id,src_unique_name,targetLocation,runId);
				completionServiceMetadata.submit(exportMetadataCallable);
			}
			
			int received = 0;

			while(received < tableInfo.size() ) {
				try {
					Future<String> resultFuture = completionServiceMetadata.take(); //blocks if none available
					String result = resultFuture.get();
					received ++;

					logger.info("Metadata exported for : "+result);
				}
				catch(InterruptedException e) {
					e.printStackTrace();
					received++;
				} catch (ExecutionException e) {
					e.printStackTrace();
					received++;
				}
			}
			
			CompletionService<String> completionServiceData = new ExecutorCompletionService<String>(pool);
			
			ExportDataCallable exportDataCallable;
			for(Map<String, String> tblName : tableInfo) {
				exportDataCallable = new ExportDataCallable(tblName.get("table_name"),connectionString,userName,password,src_sys_id,src_unique_name,targetLocation,runId);
				completionServiceData.submit(exportDataCallable);
			}

			received = 0;

			while(received < tableInfo.size() ) {
				try {
					Future<String> resultFuture = completionServiceData.take(); //blocks if none available
					String result = resultFuture.get();
					received ++;

					logger.info("Data exported for : "+result);
				}
				catch(InterruptedException e) {
					e.printStackTrace();
					received++;
				} catch (ExecutionException e) {
					e.printStackTrace();
					received++;
				}
			}
			pool.shutdown();
			
			createSystemDetails(src_unique_name,targetLocation, src_sys_id,runId);
			return "Success";
		}
		
		private static void createSystemDetails(String src_unique_name, String strg_location, String src_system_id, String runId) throws IOException {
			/*String key=authUtils.fetchKey(GenericConstants.PROJECTID, KmsConstants.LOCATIONID, KmsConstants.KEYRINGID, KmsConstants.CRYPTOKEYID, serviceAccountName);
			Credentials userCredential=authUtils.getCredentials(key);
			Storage storage=authUtils.getStorageService(userCredential);
			String bucketName = bucket;*/
			File home = new File(System.getProperty("user.home")+"/"+strg_location+"/"+src_system_id+"-"+src_unique_name+"/"+runId+"/METADATA/");
			StringBuffer stringBuffer = new StringBuffer();
			stringBuffer.append("\n");
			stringBuffer.append(src_unique_name+","+","+src_system_id+"ORACLE"+","+"ORACLE" + " FEED"+","+"to-be-decided"+","+"gs://"+"to-be-decided"+"/"+src_system_id+"-"+src_unique_name+"/"+"date"+"/"+runId+"/DATA"+","+"to-be-decided"+","
					+","+","+"MC");

	/*		BlobId file_blob_id = BlobId.of(bucketName,Integer.toString(src_sys_id)+"-"+src_unique_name+"/"+date+"/"+runId +"/METADATA/"+"mstr_sys_dtls.csv");
			BlobInfo blobInfo = BlobInfo.newBuilder(file_blob_id).setContentType("text/csv").build();*/

			Files.write(Paths.get(home.getAbsolutePath()+"/"+"mstr_sys_dtls.csv"), stringBuffer.toString().getBytes());
			
		}
		
		private static String getRunId() {
			String timeStamp = new SimpleDateFormat("ddMMyyyyHHmmss").format(new Timestamp(System.currentTimeMillis()));
			return timeStamp;
		}
	}


