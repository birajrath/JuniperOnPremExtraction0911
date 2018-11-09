package com.dataextract.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dataextract.constants.ComposerOperatorConstants;
import com.dataextract.constants.GenericConstants;
import com.dataextract.constants.KmsConstants;
import com.dataextract.dto.ConnectionDto;
import com.dataextract.dto.DataExtractDto;
import com.google.auth.Credentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;


@Repository
public class ScheduleUtilsImpl implements ScheduleUtils{
	
	@Autowired
	private AuthUtils authUtils;
	
	
	@Override
	public String composerDataExtractDagCreator(ConnectionDto connDto,DataExtractDto extractDto,String connectionUrl,Long runId,String date,String dagLocation){
		
		
		String currentdate="";
		String src_sys_id_str=String.valueOf(extractDto.getSrc_sys_id());
		Date todayDate = new Date();
		LocalDate localDate = todayDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			
			int year  = localDate.getYear();
			int month = localDate.getMonthValue();
			int day   = localDate.getDayOfMonth();

			currentdate=year+","+month+","+day;
			StringBuffer temp=new StringBuffer();
			for(Map<String,String> table: extractDto.getTableInfo()) {
				
				 for (Map.Entry<String,String> entry : table.entrySet()) {
					 temp.append(entry.getKey()+"="+entry.getValue()+"~");
				 }
				 temp.setLength(temp.length()-1);
				 temp.append("|");
			}
			temp.setLength(temp.length()-1);
			String tableInfoStr=temp.toString();
			System.out.println("tableInfoString**********************");
			System.out.println(tableInfoStr);
			//String schedule = "@"+extractDto.getCron().toLowerCase();

			StringBuffer stringBuffer = new StringBuffer();

			stringBuffer.append(ComposerOperatorConstants.IMPORTS);
			stringBuffer.append("\n\n");
			
			String default_args=ComposerOperatorConstants.DEFAULT_ARGS.replace("${date}", currentdate);
			//default_args=default_args.replace("${schedule}", schedule);
			
			stringBuffer.append(default_args);
			stringBuffer.append("\n\n");

			stringBuffer.append(ComposerOperatorConstants.DAG.replace("${dag_name}",extractDto.getSrc_unique_name()+"_Extraction")
					.replace("{$cron}", extractDto.getCron()));
			stringBuffer.append("\n\n");

			
			String operator1 = ComposerOperatorConstants.BASH_OPERATOR;
			operator1 = operator1.replace("${task_number}",extractDto.getSrc_unique_name()+"_0001");
			operator1 = operator1.replace("${task_name}", extractDto.getSrc_unique_name()+"_DataExtract");
			operator1 = operator1.replace("${script_path}", "/home/airflow/gcs/dags/code/DataExtractionBatch-0.0.1-SNAPSHOT.jar com.infy.gcp.DataExtractionBatch.NifiConfig"+" "+"\""+tableInfoStr+"\""+" "+"\""+connectionUrl+"\""+" "+"\""+connDto.getUserName()+"\""+" "+"\""+connDto.getPassword()+"\""+" "+"\""+extractDto.getBucket()+"\""+" "+"\""+src_sys_id_str+"\""+" "+"\""+extractDto.getSrc_unique_name()+"\""+" "+"\""+connDto.getConn_type()+"\""+" "+"\""+GenericConstants.PROJECTID+"\""+" "+"\""+extractDto.getService_account_name()+"\"");
			stringBuffer.append(operator1+"\n");
			stringBuffer.append("\n");
			

			//System.out.println(stringBuffer.toString());
			//Writer bufferedWriter = null;

			try {
				//Creating a file
				
				String key=authUtils.fetchKey(GenericConstants.PROJECTID, KmsConstants.LOCATIONID, KmsConstants.KEYRINGID, KmsConstants.CRYPTOKEYID, extractDto.getService_account_name());
				Credentials userCredential=authUtils.getCredentials(key);
				Storage storage=authUtils.getStorageService(userCredential);
				String bucketName = dagLocation;
				
				BlobId file_blob_id = BlobId.of(bucketName,"dags/" + extractDto.getSrc_unique_name()+"_extraction"+".py");
				BlobInfo blobInfo = BlobInfo.newBuilder(file_blob_id).setContentType("plain/text").build();
				storage.create(blobInfo, stringBuffer.toString().getBytes());
				return "success";
				
			} catch (Exception e) {
				return (e.getMessage());
			} 
		}
		
	}
	
	

