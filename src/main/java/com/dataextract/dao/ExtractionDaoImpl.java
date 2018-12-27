package com.dataextract.dao;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import javax.crypto.SecretKey;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.dataextract.constants.EncryptionConstants;
import com.dataextract.constants.OracleConstants;
import com.dataextract.constants.SchedulerConstants;
import com.dataextract.constants.StagingServerConstants;
import com.dataextract.dto.ConnectionDto;
import com.dataextract.dto.FieldMetadataDto;
import com.dataextract.dto.FileInfoDto;
import com.dataextract.dto.FileMetadataDto;
import com.dataextract.dto.HDFSMetadataDto;
import com.dataextract.dto.HiveDbMetadataDto;
import com.dataextract.dto.RealTimeExtractDto;
import com.dataextract.dto.ScheduleExtractDto;
import com.dataextract.dto.FeedDto;
import com.dataextract.dto.TableInfoDto;
import com.dataextract.dto.TableMetadataDto;
import com.dataextract.dto.TargetDto;
import com.dataextract.fetchdata.IExtract;
import com.dataextract.util.EncryptUtils;
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
	private EncryptUtils encryptUtil;

	
	
	
	
	
	@Override
	public String testConnection(ConnectionDto dto) throws Exception{
		


		String connectionUrl;
		//Connection connection=null;

		if(dto.getConn_type().equalsIgnoreCase("ORACLE"))
		{
			connectionUrl="jdbc:oracle:thin:@"+dto.getHostName()+":"+dto.getPort()+"/"+dto.getServiceName();
			try {
				Class.forName(OracleConstants.ORACLE_DRIVER);
				//Connection connection = DriverManager.getConnection(connectionUrl, dto.getUserName(), dto.getPassword());
				System.out.println("connected to source Database"); 
				//connection.close();
				return "success";

			} catch (Exception e) {
				e.printStackTrace();
				return "failed due to exception " + e.getMessage();
			}

		}
		
		if(dto.getConn_type().equalsIgnoreCase("MSSQL"))
		{
			connectionUrl="jdbc:sqlserver://"+ dto.getHostName()+":" +dto.getPort()+";DatabaseName="+dto.getDbName() ;
			try {
				/*Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				Connection connection = DriverManager.getConnection(connectionUrl, dto.getUserName(), dto.getPassword());
				System.out.println("connected to source Database"); 
				connection.close();*/
				return "success";

			} catch (Exception e) {
				e.printStackTrace();
				return "failed due to exception " + e.getMessage();
			}

		}
		

		if(dto.getConn_type().equalsIgnoreCase("TERADATA"))
		{

			return "success";

		} 

		if(dto.getConn_type().equalsIgnoreCase("UNIX")) {

			return "success";
		}
		
		if(dto.getConn_type().equalsIgnoreCase("HADOOP")) {
				return "success";
		}
		
		
		return ("invalid source type");

	
	}
	
	@Override
	public String testHiveConnection(ConnectionDto dto) throws Exception {
		


		StringBuffer dbTables=new StringBuffer();
		String message=null;
		Connection con =null;
			try {
				Class.forName(OracleConstants.HIVE_DRIVER);
				/*org.apache.hadoop.conf.Configuration hdpConfig = new org.apache.hadoop.conf.Configuration();
				hdpConfig.set("hadoop.security.authentication", "Kerberos");
				UserGroupInformation.setConfiguration(hdpConfig);*/
				con = DriverManager.getConnection("jdbc:hive2://"+dto.getHostName()+":"+dto.getPort()+"/;ssl=true;sslTrustStore="+dto.getTrust_store_path()+";trustStorePassword="+dto.getTrust_store_password()+";transportMode=http;httpPath="+dto.getKnox_gateway()+"",""+dto.getUserName()+"",""+dto.getPassword()+"");
				Statement stmt = con.createStatement();
				String sql = ("show databases");
				ResultSet res=null;
				res = stmt.executeQuery(sql);
				if (res!=null){
					while(res.next()) {
						
						Statement stmt2=con.createStatement();
						ResultSet res2=stmt2.executeQuery("show tables");
						if(res2.isBeforeFirst()) {
							while(res2.next()) {
								dbTables.append(res.getString(1)+"~"+res2.getString(1)+",");
							}
						}
						
					}
				dbTables.setLength(dbTables.length()-1);
				
				
					System.out.println("hive connection successful"); 
					message= dbTables.toString();
				}
				
				else {
					message="Failed";
				}
					
			
			
			return message;
	
		
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed";
			
		}finally {
			con.close();
		}
	
		
	}

	@Override
	public  String  insertConnectionMetadata(Connection conn, ConnectionDto dto) throws SQLException  {

		int system_sequence=0;
		int project_sequence=0;

		try {
			system_sequence=getSystemSequence(conn,dto.getSystem());
			project_sequence=getProjectSequence(conn,dto.getProject());
		}catch(SQLException e) {
			e.printStackTrace();
			return "Error retrieving system and project details";
		}

		String insertConnDetails="";
		String sequence="";
		String connectionId="";
		byte[] encrypted_key=null;
		byte[] encrypted_password=null;
		PreparedStatement pstm=null;
		if(system_sequence!=0 && project_sequence!=0) {

			if(dto.getConn_type().equalsIgnoreCase("ORACLE")|| dto.getConn_type().equalsIgnoreCase("HADOOP")||
					dto.getConn_type().equalsIgnoreCase("TERADATA")|| dto.getConn_type().equalsIgnoreCase("MSSQL")) 
			{
				
				encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
				if(encrypted_key==null) {

					return "Encryption key not found";
				}
				else {
					encrypted_password=encryptPassword(encrypted_key,dto.getPassword());
					if(encrypted_password==null) {
						return "Error while encrypting password";
					}

				}
				
				insertConnDetails="insert into "+OracleConstants.CONNECTIONTABLE+
						"(src_conn_name,src_conn_type,host_name,port_no,"
						+ "username,password,encrypted_encr_key,database_name,service_name,"
						+ "system_sequence,project_sequence,created_by) "
						+ "values(?,?,?,?,?,?,?,?,?,?,?,?)";
				pstm = conn.prepareStatement(insertConnDetails);
				pstm.setString(1, dto.getConn_name());
				pstm.setString(2, dto.getConn_type());
				pstm.setString(3, dto.getHostName());
				pstm.setString(4, dto.getPort());
				pstm.setString(5, dto.getUserName());
				pstm.setBytes(6,encrypted_password);
				pstm.setBytes(7,encrypted_key);
				pstm.setString(8, dto.getDbName());
				pstm.setString(9, dto.getServiceName());
				pstm.setInt(10, system_sequence);
				pstm.setInt(11, project_sequence);
				pstm.setString(12, dto.getJuniper_user());

				try {
					pstm.executeUpdate();
					pstm.close();
				}catch(SQLException e) {
					return e.getMessage();
				}
				
				try {	
					Statement statement=conn.createStatement();
					String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.CONNECTIONTABLE).replace("${columnName}", OracleConstants.CONNECTIONTABLEKEY);
					ResultSet rs=statement.executeQuery(query);
					if(rs.isBeforeFirst()){
						rs.next();
						sequence=rs.getString(1).split("\\.")[1];
						rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
						if(rs.isBeforeFirst()){
							rs.next();
							connectionId=rs.getString(1);
						}
					}	
				}catch(SQLException e) {
					e.printStackTrace();
					return e.getMessage();
				}finally {
					conn.close();
				}



				return "success:"+connectionId;


			}

			if(dto.getConn_type().equalsIgnoreCase("UNIX")) {
				insertConnDetails=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.CONNECTIONTABLE)
						.replace("{$columns}", "src_conn_name,src_conn_type,drive_sequence,system_sequence,project_sequence,created_by")
						.replace("{$data}",OracleConstants.QUOTE+dto.getConn_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+dto.getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
								+dto.getDrive_id()+OracleConstants.COMMA
								+system_sequence+OracleConstants.COMMA
								+project_sequence+OracleConstants.COMMA
								+OracleConstants.QUOTE+dto.getJuniper_user()+OracleConstants.QUOTE);
				try {
					Statement statement=conn.createStatement();
					statement.executeUpdate(insertConnDetails);
				}catch(SQLException e) {
					e.printStackTrace();
					return e.getMessage();
				}
				
				
				try {	
					Statement statement=conn.createStatement();
					String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.CONNECTIONTABLE).replace("${columnName}", OracleConstants.CONNECTIONTABLEKEY);
					ResultSet rs=statement.executeQuery(query);
					if(rs.isBeforeFirst()){
						rs.next();
						sequence=rs.getString(1).split("\\.")[1];
						rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
						if(rs.isBeforeFirst()){
							rs.next();
							connectionId=rs.getString(1);
						}
					}	
				}catch(SQLException e) {
					e.printStackTrace();
					return e.getMessage();
				}finally {
					conn.close();
				}



				return "success:"+connectionId;
			}
			else {
				
				return "Invalid Source";
			}

				
		}
		else {
			return "system or project does not exist";
		}

		
	}

	@SuppressWarnings("unchecked")
	private byte[] getEncryptedKey(Connection conn,int system_sequence, int project_sequence) throws SQLException {

		System.out.println("system sequence ="+system_sequence+" project sequence="+project_sequence);
		JSONObject json=new JSONObject();
		json.put("system", Integer.toString(system_sequence));
		json.put("project", Integer.toString(project_sequence));
		try {
			String response=invokeEncryption(json,EncryptionConstants.ENCRYPTIONSERVICEURL);
			System.out.println("response is "+response);
			JSONObject jsonResponse = (JSONObject) new JSONParser().parse(response);
			if(jsonResponse.get("status").toString().equalsIgnoreCase("FAILED")) {
				return null;
			}
			else {

				String query="select key_value from "+OracleConstants.KEYTABLE+" where system_sequence="+system_sequence+
						" and project_sequence="+project_sequence;
				byte[] encrypted_key=null;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {

					rs.next();
					encrypted_key=rs.getBytes(1);
					return encrypted_key;

				}
				else {
					return null;
				}

			}

		}catch (UnsupportedOperationException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	private byte[] encryptPassword(byte[] encrypted_key, String password) {

		try {
			String content = encryptUtil.readFile(EncryptionConstants.masterKeyLocation);
			SecretKey secKey = encryptUtil.decodeKeyFromString(content);
			String decrypted_key=encryptUtil.decryptText(encrypted_key,secKey);
			byte[] encrypted_password=encryptUtil.encryptText(password,decrypted_key);
			return encrypted_password;


		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public String decyptPassword(byte[] encrypted_key, byte[] encrypted_password) throws Exception {

		String content = encryptUtil.readFile(EncryptionConstants.masterKeyLocation);
		SecretKey secKey = encryptUtil.decodeKeyFromString(content);
		String decrypted_key=encryptUtil.decryptText(encrypted_key,secKey);
		SecretKey secKey2 = encryptUtil.decodeKeyFromString(decrypted_key);
		String password=encryptUtil.decryptText(encrypted_password,secKey2);
		return password;

	}








	private int getSystemSequence(Connection conn, String system_name) throws SQLException {
		// TODO Auto-generated method stub
		String query="select system_sequence from "+OracleConstants.SYSTEMTABLE+" where system_name='"+system_name+"'";
		int sys_seq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			sys_seq=rs.getInt(1);

		}
		return sys_seq;

	}

	private int getProjectSequence(Connection conn, String project) throws SQLException {
		// TODO Auto-generated method stub
		String query="select project_sequence from "+OracleConstants.PROJECTTABLE+" where project_id='"+project+"'";
		int proj_seq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			proj_seq=rs.getInt(1);


		}
		System.out.println("project sequence is "+proj_seq);
		return proj_seq;
	}

	private int getGcpSequence(Connection conn, String project, String service_account, String target_bucket) throws SQLException {
		// TODO Auto-generated method stub
		String query="select gcp_sequence from "+OracleConstants.GCPTABLE+" where gcp_project='"+project+"' and service_account_name='"+service_account
				+"' and bucket_name='"+target_bucket+"'";
		int gcp_seq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			gcp_seq=rs.getInt(1);


		}

		return gcp_seq;
	}






	@Override
	public String updateConnectionMetadata(Connection conn, ConnectionDto connDto) throws SQLException{

		String updateConnectionMaster="";
		PreparedStatement pstm=null;
		int system_sequence=0;
		int project_sequence=0;
		byte[] encrypted_key=null;
		byte[] encrypted_password=null;

		try {
			system_sequence=getSystemSequence(conn,connDto.getSystem());
			project_sequence=getProjectSequence(conn,connDto.getProject());
		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving system or project Details";
		}
		
		if(system_sequence!=0 && project_sequence!=0) {
			
			if(connDto.getConn_type().equalsIgnoreCase("ORACLE")||connDto.getConn_type().equalsIgnoreCase("HADOOP")
					||connDto.getConn_type().equalsIgnoreCase("TERADATA")) {
				
				if(!(connDto.getPassword()==null||connDto.getPassword().isEmpty())) {
					
					encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
					if(encrypted_key==null) {

						return "Encryption key not found";
					}
					else {
						encrypted_password=encryptPassword(encrypted_key,connDto.getPassword());
						if(encrypted_password==null) {
							return "Error while encrypting password";
						}

					}
					
				}

				updateConnectionMaster="update "+OracleConstants.CONNECTIONTABLE
						+" set src_conn_name=?"+OracleConstants.COMMA
						+"src_conn_type=?"+OracleConstants.COMMA
						+"host_name=?"+OracleConstants.COMMA
						+"port_no=?"+OracleConstants.COMMA
						+"username=?"+OracleConstants.COMMA
						+"password=?"+OracleConstants.COMMA 
						+"encrypted_encr_key=?"+OracleConstants.COMMA
						+"database_name=?"+OracleConstants.COMMA
						+"service_name=?"+OracleConstants.COMMA
						+"system_sequence=?"+OracleConstants.COMMA
						+"project_sequence=?"+OracleConstants.COMMA
						+"updated_by=?"
						+" where src_conn_sequence="+connDto.getConnId();
				
				try {	
					pstm = conn.prepareStatement(updateConnectionMaster);
					pstm.setString(1, connDto.getConn_name());
					pstm.setString(2, connDto.getConn_type());
					pstm.setString(3, connDto.getHostName());
					pstm.setString(4, connDto.getPort());
					pstm.setString(5, connDto.getUserName());
					pstm.setBytes(6,encrypted_password);
					pstm.setBytes(7,encrypted_key);
					pstm.setString(8, connDto.getDbName());
					pstm.setString(9, connDto.getServiceName());
					pstm.setInt(10, system_sequence);
					pstm.setInt(11, project_sequence);
					pstm.setString(12, connDto.getJuniper_user());
					
					pstm.executeUpdate();
					pstm.close();
					return "Success";

				}catch (SQLException e) {

					return e.getMessage();


				}finally {
					conn.close();
				}
			}

			if(connDto.getConn_type().equalsIgnoreCase("UNIX")) {

				updateConnectionMaster="update "+OracleConstants.CONNECTIONTABLE
						+" set src_conn_name="+OracleConstants.QUOTE+connDto.getConn_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"src_conn_type="+OracleConstants.QUOTE+connDto.getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"drive_id="+connDto.getDrive_id()+OracleConstants.COMMA
						+"system_sequence="+system_sequence+OracleConstants.COMMA
						+"project_sequence="+project_sequence+OracleConstants.COMMA
						+"updated_by="+OracleConstants.QUOTE+connDto.getJuniper_user()+OracleConstants.QUOTE
						+" where src_conn_sequence="+connDto.getConnId();
				
				return "success";
			}

			else {
				return "Invalid source";
			}

			
		}else{
			
			return "System or Project does not Exist";
		}


		
	}

	@Override
	public String deleteConnectionMetadata(Connection conn, ConnectionDto connDto)throws SQLException{
		String deleteConnectionMaster="delete from "+OracleConstants.CONNECTIONTABLE+" where SRC_CONN_SEQUENCE="+connDto.getConnId();
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
	public String insertHiveMetadata(Connection conn,ConnectionDto dto,String dbTables) throws SQLException{
		
		int system_sequence=0;
		int project_sequence=0;
		byte[] encrypted_key=null;
		byte[] encrypted_password=null;
		byte[] trust_store_encrypted_password=null;
		String insertConnDetails="";
		PreparedStatement pstm=null;
		String sequence="";
		String connectionId="";
		
		
		try {
			 system_sequence=getSystemSequence(conn,dto.getSystem());
			 project_sequence=getProjectSequence(conn,dto.getProject());
		}catch(SQLException e) {
			e.printStackTrace();
			return "Error retrieving system and project details";
		}
		
		if(system_sequence!=0 && project_sequence!=0) {
			
			encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
			if(encrypted_key==null) {

				return "Encryption key not found";
			}
			else {
				encrypted_password=encryptPassword(encrypted_key,dto.getPassword());
				trust_store_encrypted_password=encryptPassword(encrypted_key,dto.getTrust_store_password());	
			}
			insertConnDetails="insert into "+OracleConstants.CONNECTIONTABLE+
					"(src_conn_name,src_conn_type,host_name,"
					+ "port_no,username,password,encrypted_encr_key,system_sequence,"
					+ "project_sequence,created_by,knox_gateway,trust_store_path,trust_store_password,job_type) "
					+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			pstm = conn.prepareStatement(insertConnDetails);
			pstm.setString(1, dto.getConn_name());
			pstm.setString(2, dto.getConn_type());
			pstm.setString(3, dto.getHostName());
			pstm.setString(4, dto.getPort());
			pstm.setString(5, dto.getUserName());
			pstm.setBytes(6,encrypted_password);
			pstm.setBytes(7,encrypted_key);
			pstm.setInt(8, system_sequence);
			pstm.setInt(9, project_sequence);
			pstm.setString(10, dto.getJuniper_user());
			pstm.setString(11, dto.getKnox_gateway());
			pstm.setString(12, dto.getTrust_store_path());
			pstm.setBytes(13, trust_store_encrypted_password);
			pstm.setString(14, "P");

			try {
				pstm.executeUpdate();
				pstm.close();
			}catch(SQLException e) {
				e.printStackTrace();
				return e.getMessage();
			}
			
			try {	
				Statement statement=conn.createStatement();
				String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.CONNECTIONTABLE).replace("${columnName}", OracleConstants.CONNECTIONTABLEKEY);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()){
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()){
						rs.next();
						connectionId=rs.getString(1);
						for(String entry:dbTables.split(",")) {
							
							String insertTableList=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.HIVETABLELIST)
									.replace("{$columns}", "src_conn_sequence,db_name,table_name")
									.replace("{$data}",connectionId+OracleConstants.COMMA
											+OracleConstants.QUOTE+entry.split("~")[0]+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+entry.split("~")[1]+OracleConstants.QUOTE);
							try {
								statement.executeUpdate(insertTableList);
							}catch(SQLException e) {
								e.printStackTrace();
								return "Error occured while loading Hive DB/Table list";
							}
							
						}

					}
				}	
			}catch(SQLException e) {
				e.printStackTrace();
				return e.getMessage();
			}finally {
				conn.close();
			}



			return "success:"+connectionId;
			
			
		}else {
			
			return "System/Project is invalid";
			
		}
		
		

		
		
	
	}


	@Override
	public String insertTargetMetadata(Connection conn, TargetDto target) throws SQLException {


		int system_sequence=0;
		int project_sequence=0;
		int gcp_sequence=0;
		String insertTargetDetails="";
		String sequence="";
		String targetId="";
		Statement statement = conn.createStatement();
		PreparedStatement pstm=null;



		if(target.getTarget_type().equalsIgnoreCase("gcs")) {

			try {

				system_sequence=getSystemSequence(conn, target.getSystem());
				project_sequence=getProjectSequence(conn,target.getProject());
				gcp_sequence=getGcpSequence(conn,target.getTarget_project(),target.getService_account(),target.getTarget_bucket());

			}catch (SQLException e) {

				e.printStackTrace();
				return "Error Retrieving system/project/gcp target details";
			}
			if(system_sequence!=0 && project_sequence!=0&& gcp_sequence!=0) {
				
				/*byte[] encrypted_key=null;
				byte[] encrypted_sa_key=null;
				encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
				if(encrypted_key==null) 
				{

					return "Error ocurred while fetching  key for encryption";
				}
				else 
				{
					encrypted_sa_key=encryptPassword(encrypted_key,target.getService_account_key());
					if(encrypted_sa_key==null) {
						return "Error while encrypting password";
					}

				}
				
				String updateGcpDetails="update "+OracleConstants.GCPTABLE+" set SERVICE_ACCOUNT_KEY=? , ENCRYPTED_ENCR_KEY=? where gcp_sequence=?";
				pstm = conn.prepareStatement(updateGcpDetails);
				pstm.setBytes(1, encrypted_sa_key);
				pstm.setBytes(2,encrypted_key);
				pstm.setInt(3, gcp_sequence);
				pstm.executeUpdate();*/
				
				
				insertTargetDetails=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TAREGTTABLE)
						.replace("{$columns}", "target_unique_name,target_type,gcp_sequence,system_sequence,project_sequence,created_by")
						.replace("{$data}", OracleConstants.QUOTE + target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_type()+OracleConstants.QUOTE+OracleConstants.COMMA
								+gcp_sequence+OracleConstants.COMMA
								+system_sequence+OracleConstants.COMMA
								+project_sequence+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getJuniper_user()+OracleConstants.QUOTE
								);
				statement.executeUpdate(insertTargetDetails);
			}
			else {
				return "System/Project/GCP details not Correct";
			}

		}
		if(target.getTarget_type().equalsIgnoreCase("hdfs")) {



			try {
				system_sequence=getSystemSequence(conn, target.getSystem());
				project_sequence=getProjectSequence(conn,target.getProject());
			}catch(SQLException e) {
				e.printStackTrace();
				return "Error while retrieving system/project details";
			}

			if(system_sequence!=0 && project_sequence!=0) {

				byte[] encrypted_key=null;
				byte[] encrypted_password=null;
				encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
				if(encrypted_key==null) 
				{

					return "Error ocurred while fetching  key for encryption";
				}
				else 
				{
					encrypted_password=encryptPassword(encrypted_key,target.getTarget_password());
					if(encrypted_password==null) {
						return "Error while encrypting password";
					}

				}
				insertTargetDetails=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TAREGTTABLE)
						.replace("{$columns}", "target_unique_name,target_type,hdp_knox_host,hdp_knox_port,hdfs_gateway,hdp_user,hdp_encrypted_password,encrypted_key,hdp_hdfs_path,materialization_flag,partition_flag,hive_gateway,system_sequence,project_sequence,created_by")
						.replace("{$data}", "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
								);

				try {
					pstm = conn.prepareStatement(insertTargetDetails);
					pstm.setString(1, target.getTarget_unique_name());
					pstm.setString(2, target.getTarget_type());
					pstm.setString(3, target.getTarget_knox_host());
					pstm.setInt(4, target.getTarget_knox_port());
					pstm.setString(5, target.getTarget_hdfs_gateway());
					pstm.setString(6, target.getTarget_user());
					pstm.setBytes(7, encrypted_password);
					pstm.setBytes(8,encrypted_key);
					pstm.setString(9, target.getTarget_hdfs_path());
					pstm.setString(10, target.getMaterialization_flag());
					pstm.setString(11,target.getPartition_flag());
					pstm.setString(12, target.getHive_gateway());
					pstm.setInt(13, system_sequence);
					pstm.setInt(14, project_sequence);
					pstm.setString(15, target.getJuniper_user());
					pstm.executeUpdate();
				}catch(SQLException e) {
					e.printStackTrace();
					return e.getMessage();
				}

			}
			else {

				return "System/Project Details are not correct";
			}

		}
		if(target.getTarget_type().equalsIgnoreCase("unix")) {

			try {
				system_sequence=getSystemSequence(conn, target.getSystem());
				project_sequence=getProjectSequence(conn,target.getProject());
			}catch(SQLException e) {
				e.printStackTrace();
				return "Error while retrieving system/project details";
			}

			if(system_sequence!=0 && project_sequence!=0) {

				insertTargetDetails=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TAREGTTABLE)
						.replace("{$columns}", "target_unique_name,target_type,drive_sequence,unix_data_path,system_sequence,project_sequence,created_by")
						.replace("{$data}", OracleConstants.QUOTE + target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getTarget_type()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getDrive_id()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getData_path()+OracleConstants.QUOTE+OracleConstants.COMMA
								+system_sequence+OracleConstants.COMMA
								+project_sequence+OracleConstants.COMMA
								+OracleConstants.QUOTE+target.getJuniper_user()+OracleConstants.QUOTE
								);
				statement.executeUpdate(insertTargetDetails);
			}
			else {

				return "System/Project Details are not correct";
			}

		}



		/*if(target.getTarget_type().equalsIgnoreCase("HIVE")) {
			try {
				system_sequence=getSystemSequence(conn, target.getSystem());
				project_sequence=getProjectSequence(conn,target.getProject());
			}catch(SQLException e) {
				e.printStackTrace();
				return "Error while retrieving system/project details";
			}

			if(system_sequence!=0 && project_sequence!=0) {

				byte[] encrypted_key=null;
				byte[] encrypted_password=null;
				byte[] trust_store_encrypted_password=null;
				encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);

				if(encrypted_key==null){
					return "Error ocurred while fetching  key for encryption";
				}
				else{
					encrypted_password=encryptPassword(encrypted_key,target.getTarget_password());
					trust_store_encrypted_password=encryptPassword(encrypted_key,target.getTrust_store_password());
					if(encrypted_password==null) {
						return "Error while encrypting password";
					}
				}
				insertTargetDetails="insert into "+OracleConstants.TAREGTTABLE+
						"(target_unique_name,target_type,hdp_knox_url,hdp_user,hdp_encrypted_password,encrypted_key,system_sequence,project_sequence,created_by,knox_gateway,trust_store_path,trust_store_password,job_type) "
						+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
				pstm = conn.prepareStatement(insertTargetDetails);
				pstm.setString(1, target.getTarget_unique_name());
				pstm.setString(2, target.getTarget_type());
				pstm.setString(3, target.getTarget_knox_url());
				pstm.setString(4, target.getTarget_user());
				pstm.setBytes(5, encrypted_password);
				pstm.setBytes(6,encrypted_key);
				pstm.setInt(7, system_sequence);
				pstm.setInt(8, project_sequence);
				pstm.setString(9, target.getJuniper_user());
				pstm.setString(10, target.getKnox_gateway());
				pstm.setString(11, target.getTrust_store_path());
				pstm.setBytes(12, trust_store_encrypted_password);
				pstm.setString(13, "P");

				try {
					pstm.executeUpdate();
					pstm.close();
				}catch(SQLException e) {
					System.out.println("eorror "+e);
					return e.getMessage();
				}
			}else {
				return "System/Project Details are not correct";
			}
		}*/

		try {	


			String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.TAREGTTABLE).replace("${columnName}", OracleConstants.TARGETTABLEKEY);
			ResultSet rs=statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				sequence=rs.getString(1).split("\\.")[1];
				rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
				if(rs.isBeforeFirst()) {
					rs.next();
					targetId=rs.getString(1);

				}
			}
			else {
				System.out.println("empty resultset");
			}


		}catch (Exception e) {

			e.printStackTrace();
			return e.getMessage();


		}finally {
			conn.close();
		}


		return ("success:"+targetId);

	}




	@Override
	public String updateTargetMetadata(Connection conn, TargetDto target) throws SQLException{


		int system_sequence=0;
		int project_sequence=0;
		int gcp_sequence=0;
		byte[] encrypted_key=null;
		byte[] encrypted_password=null;
		String updateTargetMaster="";
		PreparedStatement pstm=null;

		if(target.getTarget_type().equalsIgnoreCase("gcs")) {

			try {

				system_sequence=getSystemSequence(conn, target.getSystem());
				project_sequence=getProjectSequence(conn,target.getProject());
				gcp_sequence=getGcpSequence(conn,target.getTarget_project(),target.getService_account(),target.getTarget_bucket());

			}catch (SQLException e) {

				e.printStackTrace();
				return "Error Retrieving system/project/gcp target details";
			}

			if(system_sequence!=0 && project_sequence!=0&& gcp_sequence!=0) {
				
				
				updateTargetMaster="update "+OracleConstants.TAREGTTABLE 
						+" set target_unique_name="+OracleConstants.QUOTE+target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"gcp_sequence="+gcp_sequence+OracleConstants.COMMA
						+"system_sequence="+system_sequence+OracleConstants.QUOTE+OracleConstants.COMMA
						+"project_sequence="+project_sequence+OracleConstants.QUOTE+OracleConstants.COMMA
						+"updated_by="+OracleConstants.QUOTE+target.getJuniper_user()+OracleConstants.QUOTE
						+" where target_sequence="+target.getTarget_id();
			
			
			try {
				Statement statement = conn.createStatement();
				statement.execute(updateTargetMaster);
				return "success";
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//TODO: Log the error message
				return e.getMessage();


			}finally {
				conn.close();
			}
		}
			else {

				return "System/Project/GCP details are not correct";

			}
		}
		

		else if(target.getTarget_type().equalsIgnoreCase("hdfs")) { 


			try {

				system_sequence=getSystemSequence(conn, target.getSystem());
				project_sequence=getProjectSequence(conn,target.getProject());

			}catch (SQLException e) {

				e.printStackTrace();
				return "Error Retrieving system/project target details";
			}

			if(system_sequence!=0 && project_sequence!=0) {
				
			
					
					encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
					if(encrypted_key==null) {

						return "Encryption key not found";
					}
					else {
						encrypted_password=encryptPassword(encrypted_key,target.getTarget_password());
						if(encrypted_password==null) {
							return "Error while encrypting password";
						}

					}
					
				

				updateTargetMaster="update "+OracleConstants.TAREGTTABLE 
						+" set target_unique_name=?"+OracleConstants.COMMA
						+"hdp_knox_host=?"+OracleConstants.COMMA
						+"hdp_knox_port=?"+OracleConstants.COMMA
						+"hdfs_gateway=?"+OracleConstants.COMMA
						+"hdp_user=?"+OracleConstants.COMMA
						+"hdp_encrypted_password=?"+OracleConstants.COMMA
						+"encrypted_key=?"+OracleConstants.COMMA
						+"hdp_hdfs_path=?"+OracleConstants.COMMA
						+"materialization_flag=?"+OracleConstants.COMMA
						+"partition_flag=?"+OracleConstants.COMMA
						+"hive_gateway=?"+OracleConstants.COMMA
						+"system_sequence=?"+OracleConstants.COMMA
						+"project_sequence=?"+OracleConstants.COMMA
						+"updated_by=?"
						+" where target_conn_sequence=?";
				
				try {
					pstm = conn.prepareStatement(updateTargetMaster);
					pstm.setString(1, target.getTarget_unique_name());
					pstm.setString(2, target.getTarget_knox_host());
					pstm.setInt(3, target.getTarget_knox_port());
					pstm.setString(4, target.getTarget_hdfs_gateway());
					pstm.setString(5, target.getTarget_user());
					pstm.setBytes(6, encrypted_password);
					pstm.setBytes(7,encrypted_key);
					pstm.setString(8, target.getTarget_hdfs_path());
					pstm.setString(9, target.getMaterialization_flag());
					pstm.setString(10, target.getPartition_flag());
					pstm.setString(11, target.getHive_gateway());
					pstm.setInt(12, system_sequence);
					pstm.setInt(13, project_sequence);
					pstm.setString(14, target.getJuniper_user());
					pstm.setInt(15, target.getTarget_id());
					pstm.executeUpdate();
					return "success";
				
				}catch(SQLException e) {
					e.printStackTrace();
					return e.getMessage();
				}finally {
					conn.close();
				}

			}
			else {

				return "System/Project details are not correct";

			}




		}


		else if(target.getTarget_type().equalsIgnoreCase("unix")) {
			
			
			try {

				system_sequence=getSystemSequence(conn, target.getSystem());
				project_sequence=getProjectSequence(conn,target.getProject());
				gcp_sequence=getGcpSequence(conn,target.getTarget_project(),target.getService_account(),target.getTarget_bucket());

			}catch (SQLException e) {

				e.printStackTrace();
				return "Error Retrieving system/project/gcp target details";
			}

			if(system_sequence!=0 && project_sequence!=0&& gcp_sequence!=0) {
				
				updateTargetMaster="update "+OracleConstants.TAREGTTABLE 
						+" set target_unique_name="+OracleConstants.QUOTE+target.getTarget_unique_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"drive_id="+OracleConstants.QUOTE+target.getDrive_id()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"data_path="+OracleConstants.QUOTE+target.getData_path()+OracleConstants.QUOTE+OracleConstants.COMMA
						+"system="+OracleConstants.QUOTE+target.getSystem()+OracleConstants.QUOTE
						+" where target_sequence="+target.getTarget_id();
				
				try {
					Statement statement = conn.createStatement();
					statement.execute(updateTargetMaster);
					return "success";
				}catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					//TODO: Log the error message
					return e.getMessage();


				}finally {
					conn.close();
				}
				
			}
			else {
				
				return "System/Project details are not correct";
			}
			
			
			
			
		}
		else {
			return "Invalid target type";
		}

		

	}


	@Override
	public String insertFeedMetadata(Connection conn,FeedDto feedDto) throws SQLException{


		int project_sequence=0;
		String insertFeedMaster="";
		String insertSrcTgtLink="";
		String feed_id="";
		String sequence="";

		try {
			project_sequence=getProjectSequence(conn,feedDto.getProject());

		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving Project details";
		}

		if(project_sequence!=0) {

			insertFeedMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.FEEDTABLE)
					.replace("{$columns}", "feed_unique_name,country_code,extraction_type,project_sequence,created_by")
					.replace("{$data}", OracleConstants.QUOTE +feedDto.getFeed_name() +OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+feedDto.getCountry_code()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+feedDto.getFeed_extract_type()+OracleConstants.QUOTE+OracleConstants.COMMA
							+project_sequence+OracleConstants.COMMA
							+OracleConstants.QUOTE+feedDto.getJuniper_user()+OracleConstants.QUOTE
							);

			try {	
				Statement statement = conn.createStatement();
				statement.execute(insertFeedMaster);
				String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.FEEDTABLE).replace("${columnName}", OracleConstants.FEEDTABLEKEY);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()) {
						rs.next();
						feed_id=rs.getString(1);
						for(String target:feedDto.getTarget().split(",")) {
							insertSrcTgtLink=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SRCTGTLINKTABLE)
									.replace("{$columns}", "feed_sequence,src_conn_sequence,target_sequence,created_by")
									.replace("{$data}", feed_id+OracleConstants.COMMA
											+feedDto.getSrc_conn_id()+OracleConstants.COMMA
											+target+OracleConstants.COMMA
											+OracleConstants.QUOTE+feedDto.getJuniper_user()+OracleConstants.QUOTE
											);
							statement.execute(insertSrcTgtLink);
						}
						
					}
				}
			}catch (SQLException e) {

				e.printStackTrace();
				return(e.getMessage());


			}finally {
				conn.close();
			}
		}else {

			return "Project details are invalid";
		}

		return "Success:"+feed_id;
	}

	@Override
	public String updateFeedMetadata(Connection conn, FeedDto feedDto) throws SQLException{

		int project_sequence=0;
		String updateFeedMaster="";
		String insertSrcTgtLink="";
		String deleteSrcTgtLink="";
		try {
			project_sequence=getProjectSequence(conn,feedDto.getProject());

		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving Project details";
		}

		if(project_sequence!=0) {


			updateFeedMaster="update "+OracleConstants.FEEDTABLE 
					+" set extraction_type="+OracleConstants.QUOTE+feedDto.getFeed_extract_type()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"feed_unique_name="+OracleConstants.QUOTE+feedDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"country_code="+OracleConstants.QUOTE+feedDto.getCountry_code()+OracleConstants.QUOTE+OracleConstants.COMMA
					+"project_sequence="+project_sequence+OracleConstants.COMMA
					+"updated_by="+OracleConstants.QUOTE+feedDto.getJuniper_user()+OracleConstants.QUOTE
					+" where feed_sequence="+feedDto.getFeed_id();
			
			deleteSrcTgtLink="delete from "+OracleConstants.SRCTGTLINKTABLE
					+" where feed_sequence="+feedDto.getFeed_id();
			
			for(String target:feedDto.getTarget().split(",")) {
				insertSrcTgtLink=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SRCTGTLINKTABLE)
						.replace("{$columns}", "feed_sequence,src_conn_sequence,target_sequence,created_by")
						.replace("{$data}", feedDto.getFeed_id()+OracleConstants.COMMA
								+feedDto.getSrc_conn_id()+OracleConstants.COMMA
								+target+OracleConstants.COMMA
								+OracleConstants.QUOTE+feedDto.getJuniper_user()+OracleConstants.QUOTE
								);
			}
			
			
			
			try {	
				Statement statement = conn.createStatement();
				statement.execute(updateFeedMaster);
				statement.executeQuery(deleteSrcTgtLink);
				statement.execute(insertSrcTgtLink);
				return "Success";

			}catch (SQLException e) {
				return e.getMessage();


			}finally {
				conn.close();
			}

		}
		else {
			return "Project Details are invalid";

		}

	}

	@Override
	public String deleteFeed(Connection conn, FeedDto feedDto)throws SQLException{
		String deleteFeedMaster="delete from "+OracleConstants.FEEDTABLE+" where feed_sequence="+feedDto.getFeed_id();
		String deleteSrcTgtLink="delete from "+OracleConstants.SRCTGTLINKTABLE+" where feed_sequence="+feedDto.getFeed_id();

		try {	
			Statement statement = conn.createStatement();
			statement.executeQuery(deleteSrcTgtLink);
			statement.execute(deleteFeedMaster);
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
		String sequence="";
		int project_sequence=0;
		try {
			project_sequence=getProjectSequence(conn, tableInfoDto.getProject());
		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving project details";
		}

		if(project_sequence!=0) {
			
			if(tableInfoDto.getLoad_type().equalsIgnoreCase("ind")) {
				
				for(TableMetadataDto tableMetadata:tableInfoDto.getTableMetadataArr()) {

					String columns="";
					if(tableMetadata.getColumns().equalsIgnoreCase("*")) {
						columns="all";
					}
					else {
						columns=tableMetadata.getColumns();
					}
					String insertTableMaster= OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.TABLEDETAILSTABLE)
							.replace("{$columns}","feed_sequence,table_name,columns,fetch_type,where_clause,incr_col,view_flag,view_source_schema,project_sequence,created_by" )
							.replace("{$data}",tableInfoDto.getFeed_id() +OracleConstants.COMMA
									+OracleConstants.QUOTE+tableMetadata.getTable_name()+OracleConstants.QUOTE+OracleConstants.COMMA
									+OracleConstants.QUOTE+columns+OracleConstants.QUOTE+OracleConstants.COMMA
									+OracleConstants.QUOTE+tableMetadata.getFetch_type()+OracleConstants.QUOTE+OracleConstants.COMMA
									+OracleConstants.QUOTE+tableMetadata.getWhere_clause()+OracleConstants.QUOTE+OracleConstants.COMMA
									+OracleConstants.QUOTE+tableMetadata.getIncr_col()+OracleConstants.QUOTE+OracleConstants.COMMA
									+OracleConstants.QUOTE+tableMetadata.getView_flag()+OracleConstants.QUOTE+OracleConstants.COMMA
									+OracleConstants.QUOTE+tableMetadata.getView_source_schema()+OracleConstants.QUOTE+OracleConstants.COMMA
									+project_sequence+OracleConstants.COMMA
									+OracleConstants.QUOTE+tableInfoDto.getJuniper_user()+OracleConstants.QUOTE
									);
					try {	
						Statement statement = conn.createStatement();
						statement.executeUpdate(insertTableMaster);
						String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.TABLEDETAILSTABLE).replace("${columnName}", OracleConstants.TABLEKEY);
						ResultSet rs=statement.executeQuery(query);
						if(rs.isBeforeFirst()) {
							rs.next();
							sequence=rs.getString(1).split("\\.")[1];
							rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
							if(rs.isBeforeFirst()) {
								rs.next();
								tableIdList.append(rs.getString(1)+",");
							}
						}
						else {
							return "Error while adding table Details";
						}
						

					}catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						//TODO: Log the error message
						conn.close();
						return e.getMessage();


					}
				}
			}
			
			else {
				
				
			}
			
	
			conn.close();
			tableIdList.setLength(tableIdList.length()-1);
			return "Success:"+tableIdList;
			

		}
		else {
			conn.close();
			return "Project Details invalid";
		}


	}
	
	


	@Override
	public String insertFileMetadata(Connection conn, FileInfoDto fileInfoDto) throws SQLException{

		StringBuffer fileList=new StringBuffer();
		String insertFileMaster="";
		String insertFieldMaster="";
		String sequence="";
		String file_path=getFilePath(conn, fileInfoDto.getFeed_id(),fileInfoDto.getDataPath());
		int project_sequence=getProjectSequence(conn, fileInfoDto.getProject());
		String juniper_user=fileInfoDto.getJuniper_user();
		int file_sequence=0;


		for(FileMetadataDto file:fileInfoDto.getFileMetadataArr()) {

			String delimiter="";
			if(file.getFile_delimiter().equalsIgnoreCase("comma")) {
				delimiter=",";
			}
			else if(file.getFile_delimiter().equalsIgnoreCase("tab")) {
				delimiter="\t";
			}
			else if(file.getFile_delimiter().equalsIgnoreCase("semicolon")) {
				delimiter=";";
			}
			else if(file.getFile_delimiter().equalsIgnoreCase("pipe")) {
				delimiter="\\|";
			}
			else {
				delimiter=null;
			}
			insertFileMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.FILEDETAILSTABLE)
					.replace("{$columns}","feed_sequence,file_path,file_name,file_type,file_delimiter,"
							+ "header_count,trailer_count,avro_conv_flg,bus_dt_format,bus_dt_loc,bus_dt_start,"
							+ "count_loc,count_start,count_length,project_sequence,created_by" )
					.replace("{$data}",fileInfoDto.getFeed_id() +OracleConstants.COMMA
							+OracleConstants.QUOTE+file_path+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getFile_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getFile_type()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+delimiter+OracleConstants.QUOTE+OracleConstants.COMMA
							+file.getHeader_count()+OracleConstants.COMMA
							+file.getTrailer_count()+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getAvro_conv_flag()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getBus_dt_format()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getBus_dt_loc()+OracleConstants.QUOTE+OracleConstants.COMMA
							+file.getBus_dt_start()+OracleConstants.COMMA
							+OracleConstants.QUOTE+file.getCount_loc()+OracleConstants.QUOTE+OracleConstants.COMMA
							+file.getCount_start()+OracleConstants.COMMA
							+file.getCount_legnth()+OracleConstants.COMMA
							+project_sequence+OracleConstants.COMMA
							+OracleConstants.QUOTE+juniper_user+OracleConstants.QUOTE
							);
			try {	
				Statement statement = conn.createStatement();
				System.out.println("Query is "+insertFileMaster);
				statement.executeUpdate(insertFileMaster);
				String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.FILEDETAILSTABLE).replace("${columnName}", "FILE_SEQUENCE");
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()) {
						rs.next();
						fileList.append(rs.getString(1)+",");
						file_sequence=Integer.parseInt(rs.getString(1));
						for(FieldMetadataDto field:fileInfoDto.getFieldMetadataArr()) {
							if(field.getFile_name().equalsIgnoreCase(file.getFile_name())) {

								insertFieldMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.FIELDDETAILSTABLE)
										.replace("{$columns}","feed_sequence,file_sequence,file_name,field_pos,field_name,"
												+ "field_data_type,project_sequence,created_by" )
										.replace("{$data}",fileInfoDto.getFeed_id()+OracleConstants.COMMA
												+file_sequence+OracleConstants.COMMA
												+OracleConstants.QUOTE+file.getFile_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+field.getField_position()+OracleConstants.COMMA
												+OracleConstants.QUOTE+field.getField_name()+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+field.getField_datatype()+OracleConstants.QUOTE+OracleConstants.COMMA
												+project_sequence+OracleConstants.COMMA
												+OracleConstants.QUOTE+juniper_user+OracleConstants.QUOTE);


								statement.executeUpdate(insertFieldMaster);
							}

						}
					}

				}
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//TODO: Log the error message
				return e.getMessage();


			}


		}
		conn.close();
		return "Success";


		

	}


	@SuppressWarnings("unused")
	private String putFile(FileInfoDto fileInfoDto,String file_path) throws SQLException, SftpException {

		StringBuffer fileDetails= new StringBuffer();
		StringBuffer fieldDetails=new StringBuffer();	

		try {
			JSch obj_JSch = new JSch();
			Session obj_Session = null;
			obj_Session = obj_JSch.getSession(StagingServerConstants.stagingServerUser, StagingServerConstants.stagingServerIp);
			obj_Session.setPort(22);
			obj_Session.setPassword(StagingServerConstants.stagingServerUserPassword);
			Properties obj_Properties = new Properties();
			obj_Properties.put("StrictHostKeyChecking", "no");
			obj_Session.setConfig(obj_Properties);
			obj_Session.connect();
			Channel obj_Channel = obj_Session.openChannel("sftp");
			obj_Channel.connect();
			ChannelSftp obj_SFTPChannel = (ChannelSftp) obj_Channel;
			fileDetails.append("feed_sequence,file_name,file_type,file_delimiter,header_count,trailer_count,avro_conv_flg,bus_dt_format,bus_dt_loc,bus_dt_start,count_loc,count_start,count_length");
			fileDetails.append("\n");
			for(FileMetadataDto file:fileInfoDto.getFileMetadataArr()) {
				fileDetails.append(fileInfoDto.getFeed_id()+","+file.getFile_name()+","+file.getFile_type()+","+file.getFile_delimiter()
				+","+file.getHeader_count()+","+file.getTrailer_count()+","+file.getAvro_conv_flag()
				+","+file.getBus_dt_format()+","+file.getBus_dt_loc()+","+file.getBus_dt_start()
				+","+file.getCount_loc()+","+file.getCount_start()+","+file.getCount_legnth());

				fileDetails.append("\n");
			}
			fieldDetails.append("feed_sequence,file_id,field_pos,length,field_name,field_datatype");
			fieldDetails.append("\n");
			for(FieldMetadataDto field:fileInfoDto.getFieldMetadataArr()) {
				fieldDetails.append(fileInfoDto.getFeed_id()+","+field.getFile_name()+","+field.getField_position()+","+field.getLength()+","+field.getField_name()+","+field.getField_datatype());
				fieldDetails.append("\n");
			}
			InputStream fileInputStream = new ByteArrayInputStream(fileDetails.toString().getBytes());
			InputStream fieldInputStream=new ByteArrayInputStream(fieldDetails.toString().getBytes());
			SftpATTRS attrs=null;
			obj_SFTPChannel.cd(file_path);
			try {
				attrs=obj_SFTPChannel.stat("metadata");

			}catch(Exception e) {
				System.out.println("folder does not exists");
				obj_SFTPChannel.mkdir("metadata");
			}

			obj_SFTPChannel.cd("metadata");
			obj_SFTPChannel.put(fileInputStream, file_path+"/metadata/"+"mstr_file_dtls.csv");
			obj_SFTPChannel.put(fieldInputStream, file_path+"/metadata/"+"mstr_field_dtls.csv");
			return "success";

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return e.getMessage();
		}




	}

	private String getFilePath(Connection conn,int feed_id, String dataPath) {

		String query1="select src_conn_sequence from "+OracleConstants.SRCTGTLINKTABLE+" where feed_sequence="+feed_id;
		int conn_id;
		int drive_id;
		String drive_path;
		String file_path;
		try {
			Statement statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(query1);
			if(rs.isBeforeFirst()) {
				rs.next();
				conn_id=rs.getInt(1);
				String query2="select drive_sequence from "+OracleConstants.CONNECTIONTABLE+" where src_conn_sequence="+conn_id;
				rs=statement.executeQuery(query2);
				if(rs.isBeforeFirst()) {
					rs.next();
					drive_id=rs.getInt(1);
					String query3="select mounted_path from "+OracleConstants.DRIVETABLE+" where drive_sequence="+drive_id;
					rs=statement.executeQuery(query3);
					if(rs.isBeforeFirst()) {
						rs.next();
						drive_path=rs.getString(1);
						file_path=drive_path+dataPath;
						return file_path;
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
	public ConnectionDto getConnectionObject(Connection conn,String feed_name) throws SQLException {

		int connId=getConnectionId(conn,feed_name);
		ConnectionDto connDto=new ConnectionDto();
		String system_sequence="";
		
		String query="select src_conn_type,host_name,port_no,username,password,encrypted_encr_key,database_name,service_name,drive_sequence,knox_gateway,trust_store_path,trust_store_password,system_sequence from "+OracleConstants.CONNECTIONTABLE+ " where src_conn_sequence="+connId;
		try {
			Statement statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				connDto.setConn_type(rs.getString(1));
				connDto.setHostName(rs.getString(2));
				connDto.setPort(rs.getString(3));
				connDto.setUserName(rs.getString(4));
				connDto.setEncrypted_password(rs.getBytes(5));
				connDto.setEncr_key(rs.getBytes(6));
				connDto.setDbName(rs.getString(7));
				connDto.setServiceName(rs.getString(8));
				String drive_id=rs.getString(9);
				if(!(drive_id==null)) {

					connDto.setDrive_id(Integer.parseInt(drive_id));
				}
				connDto.setKnox_gateway(rs.getString(10));
				connDto.setTrust_store_path(rs.getString(11));
				connDto.setEncrypted_trust_store_password(rs.getBytes(12));
				connDto.setSys_seq(Integer.parseInt(rs.getString(13)));;
				
			}

		}catch(SQLException e){
			e.printStackTrace();
			throw new SQLException("Exception ocuured while retrieving connection details");

		}finally {
			conn.close();
		}
		
		return connDto;


	}

	private int getConnectionId (Connection conn,String feed_name) throws SQLException {

		int connectionId=0;
		String query="select src_conn_sequence from "+OracleConstants.SRCTGTLINKTABLE+" where feed_sequence=(select feed_sequence from "
		+OracleConstants.FEEDTABLE+" where feed_unique_name='"+feed_name+"')";
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			connectionId=rs.getInt(1);

		}
		return connectionId;

	}

	@Override
	public FeedDto getFeedObject(Connection conn,String feed_name,String src_type) throws Exception{

		FeedDto feedDto=new FeedDto();

		String query="select feed_sequence,feed_unique_name,country_code,project_sequence from "+OracleConstants.FEEDTABLE
				+ " where feed_unique_name='"+feed_name+"'";
		

		try {
			Statement statement=conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				feedDto.setFeed_id(Integer.parseInt(rs.getString(1)));
				feedDto.setFeed_name(rs.getString(2));
				feedDto.setCountry_code(rs.getString(3));
				String projectSequence=rs.getString(4);
				if(!(projectSequence==null)) {
					feedDto.setProject_sequence(Integer.parseInt(projectSequence));
				}
			}

		}catch(SQLException e) {
			e.printStackTrace();
			throw new Exception("Exception occured while retrieving feed details");
		}
		String query2="select target_sequence from "+OracleConstants.SRCTGTLINKTABLE
				+" where feed_sequence="+feedDto.getFeed_id();
		StringBuffer targetList=new StringBuffer();
		try {
			Statement statement=conn.createStatement();
			ResultSet rs = statement.executeQuery(query2);
			if(rs.isBeforeFirst()) {
				while(rs.next()) {
					targetList.append(rs.getString(1)+",");
				}
			}
		}catch(SQLException e) {
			e.printStackTrace();
			throw new Exception("Exception occured while fetching target information of the feed");
		}
		targetList.setLength(targetList.length()-1);
		feedDto.setTarget(targetList.toString());
		
		if(src_type.equalsIgnoreCase("ORACLE")||src_type.equalsIgnoreCase("TERADATA")||src_type.equalsIgnoreCase("MSSQL")) {
			StringBuffer tableList=new StringBuffer();
			String query3="select table_sequence from "+OracleConstants.TABLEDETAILSTABLE+
					" where feed_sequence="+feedDto.getFeed_id();
			try {
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query3);
				if(rs.isBeforeFirst()) {
					while(rs.next()) {
						tableList.append(rs.getString(1)+",");
					}
				}
			}catch(SQLException e) {
				e.printStackTrace();
				throw new Exception("Exception occured while fetching table information of the feed");
			}finally {
				conn.close();
			}
			tableList.setLength(tableList.length()-1);
			feedDto.setTableList(tableList.toString());
		}
		if(src_type.equalsIgnoreCase("UNIX")) {
			
			StringBuffer fileList=new StringBuffer();
			String query3="select file_sequence from "+OracleConstants.FIELDDETAILSTABLE+
					" where feed_sequence="+feedDto.getFeed_id();
			try {
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query3);
				if(rs.isBeforeFirst()) {
					while(rs.next()) {
						fileList.append(rs.getString(1)+",");
					}
				}
			}catch(SQLException e) {
				e.printStackTrace();
				throw new Exception("Exception occured while fetching File information of the feed");
			}finally {
				conn.close();
			}
			fileList.setLength(fileList.length()-1);
			feedDto.setFileList(fileList.toString());
		}
		
		if(src_type.equalsIgnoreCase("HIVE")) {
			
			String query3="select hdb_id from "+OracleConstants.DBPROPOGATIONTABLE
					+" where feed_id="+feedDto.getFeed_id();
			
			StringBuffer dbList=new StringBuffer();
			try {
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query3);
				if(rs.isBeforeFirst()) {
					while(rs.next()) {
						dbList.append(rs.getString(1)+",");
					}
				}
			}catch(SQLException e) {
				e.printStackTrace();
				throw new Exception("Exception occured while fetching Database Information of the feed");
			}finally {
				conn.close();
			}
			dbList.setLength(dbList.length()-1);
			feedDto.setDbList(dbList.toString());
		}
		
		
		return feedDto;

	}

	@Override
	public ArrayList<TargetDto> getTargetObject(Connection conn,String targetList) throws Exception{

		ArrayList<TargetDto> targetArr=new ArrayList<TargetDto>();
		Statement statement=conn.createStatement();
		String query="";
		ResultSet rs;
		String[] targets=targetList.split(",");;
		try {
			for(String target:targets) {
				TargetDto targetDto=new TargetDto();
				query=" select target_unique_name,target_type,gcp_sequence,hdp_knox_host,hdp_user,hdp_encrypted_password,encrypted_key,hdp_hdfs_path,drive_sequence,unix_data_path,hdfs_gateway from "+OracleConstants.TAREGTTABLE
						+ " where target_conn_sequence='"+target+"'";
				rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					if(rs.getString(2).equalsIgnoreCase("gcs")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						int gcp_seq=Integer.parseInt(rs.getString(3));
						String query2="select gcp_project,bucket_name,service_account_name from "+OracleConstants.GCPTABLE+" where gcp_sequence="+gcp_seq;
						Statement statement2=conn.createStatement();
						ResultSet rs2=statement2.executeQuery(query2);
						if(rs2.isBeforeFirst()) {
							rs2.next();
							targetDto.setTarget_project(rs2.getString(1));
							targetDto.setTarget_bucket(rs2.getString(2));
							targetDto.setService_account(rs2.getString(3));
						}

					}
					if(rs.getString(2).equalsIgnoreCase("hdfs")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						targetDto.setTarget_knox_host(rs.getString(4));
						targetDto.setTarget_user(rs.getString(5));
						targetDto.setEncrypted_password(rs.getBytes(6));
						targetDto.setEncrypted_key(rs.getBytes(7));
						targetDto.setTarget_hdfs_path(rs.getString(8));
					}
					if(rs.getString(2).equalsIgnoreCase("unix")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						targetDto.setDrive_id(rs.getString(9));
						targetDto.setData_path(rs.getString(10));
						String drivePath=getDrivePath(conn,targetDto.getDrive_id());
						targetDto.setFull_path(drivePath+targetDto.getData_path());
					}
					
					if(rs.getString(2).equalsIgnoreCase("hive")) {
						targetDto.setTarget_unique_name(rs.getString(1));
						targetDto.setTarget_type(rs.getString(2));
						targetDto.setTarget_knox_host(rs.getString(4));
						targetDto.setTarget_user(rs.getString(5));
						targetDto.setEncrypted_password(rs.getBytes(6));
						targetDto.setEncrypted_key(rs.getBytes(7));
						targetDto.setKnox_gateway(rs.getString(11));
						targetDto.setTrust_store_path(rs.getString(12));
						targetDto.setEncrypted_trust_store_password(rs.getBytes(13));
					}
				}
				targetArr.add(targetDto);

			}
		}catch(SQLException e) {
			e.printStackTrace();
			throw new Exception("Exception Occured while retrieving target details");
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
		ArrayList<TableMetadataDto> tableMetadataArr=new ArrayList<TableMetadataDto>();
		String[] tableIds=table_list.split(",");
		try {
			for(String tableId:tableIds) {
				String query="select table_name,columns,where_clause,fetch_type,incr_col,view_flag,view_source_schema from "+OracleConstants.TABLEDETAILSTABLE+" where table_sequence="+tableId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					TableMetadataDto tableMetadata=new TableMetadataDto();
					tableMetadata.setTable_name(rs.getString(1));
					tableMetadata.setColumns(rs.getString(2));
					tableMetadata.setWhere_clause( rs.getString(3));
					tableMetadata.setFetch_type(rs.getString(4));
					tableMetadata.setIncr_col(rs.getString(5));
					tableMetadata.setView_flag(rs.getString(6));
					tableMetadata.setView_source_schema(rs.getString(7));
					tableMetadataArr.add(tableMetadata);

				}


			}
			for(TableMetadataDto table:tableMetadataArr) {
				if(table.getFetch_type().equalsIgnoreCase("incr")) {
					tableInfoDto.setIncr_flag("Y");
					break;
				}
				else {
					tableInfoDto.setIncr_flag("N");
				}
			}
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}

		tableInfoDto.setTableMetadataArr(tableMetadataArr);
		return tableInfoDto;
	}

	@Override
	public FileInfoDto getFileInfoObject(Connection conn, String fileList) throws SQLException{
		FileInfoDto fileInfoDto= new FileInfoDto();
		ArrayList<FileMetadataDto> fileMetadataArr=new ArrayList<FileMetadataDto>();

		String[] fileIds=fileList.split(",");
		try {
			for(String fileId:fileIds) {
				StringBuffer fieldList=new StringBuffer();
				FileMetadataDto fileMetadataDto= new FileMetadataDto();
				String query="select file_path,file_name,file_type,file_delimiter,header_count,trailer_count,"
						+ "avro_conv_flg,bus_dt_format,bus_dt_loc,bus_dt_start,"
						+ "count_loc,count_start,count_length from "+OracleConstants.FILEDETAILSTABLE+" where file_sequence="+fileId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					fileMetadataDto.setFile_sequence(Integer.parseInt(fileId));
					fileMetadataDto.setFile_path(rs.getString(1));
					fileMetadataDto.setFile_name(rs.getString(2));
					fileMetadataDto.setFile_type(rs.getString(3));
					fileMetadataDto.setFile_delimiter(rs.getString(4));
					fileMetadataDto.setHeader_count(rs.getString(5));
					fileMetadataDto.setTrailer_count(rs.getString(6));
					fileMetadataDto.setAvro_conv_flag(rs.getString(7));
					fileMetadataDto.setBus_dt_format(rs.getString(8));
					fileMetadataDto.setBus_dt_loc(rs.getString(9));
					fileMetadataDto.setBus_dt_start(Integer.parseInt(rs.getString(10)));
					fileMetadataDto.setCount_loc(rs.getString(11));
					fileMetadataDto.setCount_start(Integer.parseInt(rs.getString(12)));
					fileMetadataDto.setCount_legnth(Integer.parseInt(rs.getString(13)));

				}


				String query2="select field_name,field_data_type from "+OracleConstants.FIELDDETAILSTABLE
						+" where file_sequence="+fileId;


				ResultSet rs2 = statement.executeQuery(query2);
				if(rs2.isBeforeFirst()) {
					while(rs2.next()) {
						fieldList.append(rs2.getString(1)+"~");
						fieldList.append(rs2.getString(2)+",");
					}
					fieldList.setLength(fieldList.length()-1);
					fileMetadataDto.setField_list(fieldList.toString());


				}

				fileMetadataArr.add(fileMetadataDto);

			}
		}catch(SQLException e) {
			e.printStackTrace();
			throw e;
		}finally {
			conn.close();
		}

		fileInfoDto.setFileMetadataArr(fileMetadataArr);
		return fileInfoDto;

	}

	@Override
	public int getProcessGroup(Connection conn, String feed_name, String country_code) throws SQLException{
		String query="select distinct nifi_pg from "+OracleConstants.NIFISTATUSTABLE+" where country_code='"+country_code+"' and feed_unique_name='"+feed_name+"'";
		try {
			Statement statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				return rs.getInt(1);
			}else {
				return 0;
			}
		}catch (SQLException e) {
			throw e;
		}finally{
			conn.close();
		}



	}

	@Override
	public String checkProcessGroupStatus(Connection conn, int index, String conn_type) throws SQLException{
		System.out.println("inside check processgroup status");
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("ddMMyyyy");
		String today = formatter.format(date);
		String query= "select status from "+ OracleConstants.NIFISTATUSTABLE +" where nifi_pg="+index+" and extracted_date='"+today+"' and pg_type='"+conn_type+"'";
		try {
			Statement statement=conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				while(rs.next()) {
					if(rs.getString(1).equalsIgnoreCase("RUNNING")) {
						return "Not Free";
					}
				}
			}else {
				return "Free";
			}
		}catch(SQLException e) {
			throw e;
		}finally {
			conn.close();
		}
		return "Free";

	}

	@SuppressWarnings("unused")
	@Override
	public  String pullData(RealTimeExtractDto rtExtractDto)  {



		String dataPull_status="";
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("ORACLE")
				||rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("TERADATA")
				||rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("MSSQL")) {
			String connectionString=getConnectionString(rtExtractDto.getConnDto());
			Long runId=getRunId();
			String date=getDate();
			try {
				dataPull_status= extract.callNifiDataRealTime(rtExtractDto,connectionString,date,runId);
				if(dataPull_status.equalsIgnoreCase("success")) {
					return "Success";
				}
				else {
					return "failed";
				}
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
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("UNIX")) {
			Long runId=getRunId();
			String date=getDate();
			try {
				dataPull_status= extract.callNifiUnixRealTime(rtExtractDto,date,runId);
				if(dataPull_status.equalsIgnoreCase("success")) {
					return "success";
				}
				else {
					return dataPull_status;
				}
				
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
				e.printStackTrace();
				return e.getMessage();
			} catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}

		}
		if(rtExtractDto.getConnDto().getConn_type().equalsIgnoreCase("HIVE")) {
			Long runId=getRunId();
			String date=getDate();
			try {
				dataPull_status= extract.callNifiHivePropagateRealTime(rtExtractDto,date,runId);
				return "success";
			} catch (UnsupportedOperationException e) {
				e.printStackTrace();
				return e.getMessage();
			} catch (Exception e) {
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
			return "jdbc:teradata://"+ connDto.getHostName() ;
		}

		return null;
	}





	@Override
	public String createDag(Connection conn,ScheduleExtractDto schDto) throws Exception{
		
		String feedId=getFeedId(conn,schDto.getFeed_name());
		schDto.setFeed_id(Integer.parseInt(feedId.split("~")[0]));
		schDto.setProject_seq(Integer.parseInt(feedId.split("~")[1]));
		ArrayList<String> tarArr= getTarObj(conn,schDto.getFeed_name(),schDto.getFeed_id());
		if(schDto.getSch_flag().equalsIgnoreCase("O")) {
			
			return scheduleOnDemandFeed(conn, schDto, tarArr);
			
		}
		if(schDto.getSch_flag().equalsIgnoreCase("R")) {
			
			return insertScheduleMetadata(conn,schDto.getFeed_name(),schDto.getProject_seq(),schDto.getCron());
		}
		
		if(schDto.getSch_flag().equalsIgnoreCase("F")) {
			
			return scheduleEventBasedFeedFile(conn,schDto,tarArr);
		}
		
		if(schDto.getSch_flag().equalsIgnoreCase("K")) {
			
			return scheduleEventBasedFeedKafka(conn,schDto,tarArr);
		}
		
		if(schDto.getSch_flag().equalsIgnoreCase("A")) {
			
			return scheduleEventBasedFeedApi(conn,schDto,tarArr);
		}

		else {
			throw new Exception("Invalid batch type");
		}
		
	}
	
	
	private String scheduleEventBasedFeedApi(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr) throws Exception {
		String insertReadJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
				.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,argument_4,schedule_type,project_id")
				.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getApi_unique_key()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+"A"+OracleConstants.QUOTE+OracleConstants.COMMA
						+schDto.getProject_seq());
		try {
			Statement statement=conn.createStatement();
			statement.executeQuery(insertReadJob);
			
		}catch(Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}
		
		for(String target:tarArr) {
			
			String insertWriteJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
					.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,PREDESSOR_JOB_ID_1,schedule_type,project_id")
					.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+target.split("~")[0]+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+"R"+OracleConstants.QUOTE+OracleConstants.COMMA
							+schDto.getProject_seq());
			
			try {
				Statement statement=conn.createStatement();
				statement.executeQuery(insertWriteJob);
				
				
			}catch(Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if(target.split("~")[1].equalsIgnoreCase("Y")) {
				String insertMatJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
						.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,PREDESSOR_JOB_ID_1,schedule_type,project_id")
						.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+"R"+OracleConstants.QUOTE+OracleConstants.COMMA
								+schDto.getProject_seq());
				try {
					Statement statement=conn.createStatement();
					statement.executeQuery(insertMatJob);
				}catch(Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			}
		}
		
		return "success";
	}

	private String scheduleEventBasedFeedKafka(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr) throws Exception {
		String insertReadJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
				.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,argument_4,schedule_type,project_id")
				.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getKafka_topic()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+"K"+OracleConstants.QUOTE+OracleConstants.COMMA
						+schDto.getProject_seq());
		try {
			Statement statement=conn.createStatement();
			statement.executeQuery(insertReadJob);
			
		}catch(Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}
		
		for(String target:tarArr) {
			
			String insertWriteJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
					.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,PREDESSOR_JOB_ID_1,schedule_type,project_id")
					.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+target.split("~")[0]+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+"R"+OracleConstants.QUOTE+OracleConstants.COMMA
							+schDto.getProject_seq());
			
			try {
				Statement statement=conn.createStatement();
				statement.executeQuery(insertWriteJob);
				
				
			}catch(Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if(target.split("~")[1].equalsIgnoreCase("Y")) {
				String insertMatJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
						.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,PREDESSOR_JOB_ID_1,schedule_type,project_id")
						.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+"R"+OracleConstants.QUOTE+OracleConstants.COMMA
								+schDto.getProject_seq());
				try {
					Statement statement=conn.createStatement();
					statement.executeQuery(insertMatJob);
				}catch(Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			}
		}
		
		return "success";
	}

	private String scheduleEventBasedFeedFile(Connection conn, ScheduleExtractDto schDto, ArrayList<String> tarArr) throws Exception {
		
		String insertReadJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
				.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,argument_4,schedule_type,project_id")
				.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFile_path()+"/"+schDto.getFile_pattern()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+"F"+OracleConstants.QUOTE+OracleConstants.COMMA
						+schDto.getProject_seq());
		try {
			Statement statement=conn.createStatement();
			statement.executeQuery(insertReadJob);
			
		}catch(Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}
		
		for(String target:tarArr) {
			
			String insertWriteJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
					.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,PREDESSOR_JOB_ID_1,schedule_type,project_id")
					.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+target.split("~")[0]+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+"R"+OracleConstants.QUOTE+OracleConstants.COMMA
							+schDto.getProject_seq());
			
			try {
				Statement statement=conn.createStatement();
				statement.executeQuery(insertWriteJob);
				
				
			}catch(Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if(target.split("~")[1].equalsIgnoreCase("Y")) {
				String insertMatJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
						.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,PREDESSOR_JOB_ID_1,schedule_type,project_id")
						.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+"R"+OracleConstants.QUOTE+OracleConstants.COMMA
								+schDto.getProject_seq());
				try {
					Statement statement=conn.createStatement();
					statement.executeQuery(insertMatJob);
				}catch(Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			}
		}
		
		return "success";
	}

	private String getFeedId(Connection conn,String feed_name) throws Exception{
		
		String query="select feed_sequence,project_sequence from "+OracleConstants.FEEDTABLE+
				" where feed_unique_name='"+feed_name+"'";
		String feed_id="";
		try {
			Statement statement = conn.createStatement();
			ResultSet rs=statement.executeQuery(query);
			if(rs.isBeforeFirst()) {
				rs.next();
				feed_id=rs.getInt(1)+"~"+rs.getInt(2);
			}
		}catch(Exception e) {
			throw new Exception("Exception Occured while retrieving feed id");
		}
		
		return feed_id;
	}
	
	
	private ArrayList<String> getTarObj(Connection conn,String feed_name,int feed_sequence) throws Exception{
		
		ArrayList<String> tarArr=new ArrayList<String>();
		String getTargetList="select t.target_unique_name,t.materialization_flag from "+OracleConstants.TAREGTTABLE
				+" t inner join "+OracleConstants.SRCTGTLINKTABLE+
				 " l on l.target_sequence=t.target_conn_sequence  where l.feed_sequence="
				+feed_sequence;
		try {
			Statement statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(getTargetList);
			if(rs.isBeforeFirst()) {
				while(rs.next()) {
					tarArr.add(rs.getString(1)+"~"+rs.getString(2));
				}
			}
			
		}catch(SQLException e) {
			e.printStackTrace();
			throw new Exception("Exception occured while fetching target of the feed");
		}
		
		return tarArr;
		
	}
	
	
	private String scheduleOnDemandFeed(Connection conn,ScheduleExtractDto schDto,ArrayList<String> tarArr) throws Exception {
		
		String insertReadJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
				.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,schedule_type,daily_flag,project_id")
				.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+"O"+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
						+schDto.getProject_seq());
		
		try {
			Statement statement=conn.createStatement();
			statement.executeQuery(insertReadJob);
			
		}catch(Exception e) {
			e.printStackTrace();
			throw new Exception("Exception occured while scheduling Read job");
		}
		
		for(String target:tarArr) {
			
			String insertWriteJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
					.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,argument_2,PREDESSOR_JOB_ID_1,schedule_type,daily_flag,project_id")
					.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+target.split("~")[0]+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+schDto.getFeed_name()+"_read"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+"O"+OracleConstants.QUOTE+OracleConstants.COMMA
							+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
							+schDto.getProject_seq());
			
			try {
				Statement statement=conn.createStatement();
				statement.executeQuery(insertWriteJob);
				
				
			}catch(Exception e) {
				e.printStackTrace();
				throw new Exception("Exception occured while scheduling write job");
			}
			if(target.split("~")[1].equalsIgnoreCase("Y")) {
				String insertMatJob=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
						.replace("{$columns}", "job_id,job_name,batch_id,feed_id,command,argument_1,PREDESSOR_JOB_ID_1,schedule_type,daily_flag,project_id")
						.replace("{$data}",OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_materialize"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_id()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+SchedulerConstants.write_script+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+schDto.getFeed_name()+"_"+target.split("~")[0]+"_write"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+"O"+OracleConstants.QUOTE+OracleConstants.COMMA
								+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
								+schDto.getProject_seq());
				try {
					Statement statement=conn.createStatement();
					statement.executeQuery(insertMatJob);
				}catch(Exception e) {
					e.printStackTrace();
					throw new Exception("Exception occured while scheduling materialization job");
				}
			}
		}
		
		return "success";
	}

	@Override
	public String updateNifiProcessgroupDetails(Connection conn, RealTimeExtractDto rtDto,String path,String date, String run_id,int index) throws SQLException{

		String insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.NIFISTATUSTABLE)
				.replace("{$columns}", "country_code,feed_id,feed_unique_name,run_id,nifi_pg,pg_type,extracted_date,project_sequence,status")
				.replace("{$data}",OracleConstants.QUOTE+rtDto.getFeedDto().getCountry_code()+OracleConstants.QUOTE+OracleConstants.COMMA
						+rtDto.getFeedDto().getFeed_id()+OracleConstants.COMMA
						+OracleConstants.QUOTE+rtDto.getFeedDto().getFeed_name()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+run_id+OracleConstants.QUOTE+OracleConstants.COMMA
						+index+OracleConstants.COMMA
						+OracleConstants.QUOTE+rtDto.getConnDto().getConn_type()+OracleConstants.QUOTE+OracleConstants.COMMA
						+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
						+rtDto.getFeedDto().getProject_sequence()+OracleConstants.COMMA
						+OracleConstants.QUOTE+"running"+OracleConstants.QUOTE);

		System.out.println("insert query is "+insertQuery);
		try {	
			Statement statement = conn.createStatement();
			statement.execute(insertQuery);
			System.out.println("query executed");
		}catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//TODO: Log the error message
			e.printStackTrace();
			return e.getMessage();
		}finally {
			conn.close();
		}
		return "success";

	}

	private  String invokeEncryption(JSONObject json,String  url) throws UnsupportedOperationException, Exception {



		CloseableHttpClient httpClient = HttpClientBuilder.create().build();


		HttpPost postRequest=new HttpPost(url);
		postRequest.setHeader("Content-Type","application/json");
		StringEntity input = new StringEntity(json.toString());
		postRequest.setEntity(input); 
		HttpResponse response = httpClient.execute(postRequest);
		HttpEntity respEntity = response.getEntity();
		return EntityUtils.toString(respEntity);
	}



	private String insertScheduleMetadata(Connection conn,String feed_name,int project_sequence,String cron) throws SQLException {

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
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
										.replace("{$data}",OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE
												+OracleConstants.COMMA+project_sequence);
								System.out.println(insertQuery);
								statement.execute(insertQuery);


							}
						}else {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
									.replace("{$data}",OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE
											+OracleConstants.COMMA+project_sequence);;
											System.out.println(insertQuery);
											statement.execute(insertQuery);
						}
					}
				}else {
					if(minutes.contains(",")) {
						for(String minute:minutes.split(",")) {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
									.replace("{$data}",OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE
											+OracleConstants.COMMA+project_sequence);
							System.out.println(insertQuery);
							statement.execute(insertQuery);


						}
					}else {
						insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
								.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
								.replace("{$data}",OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+feed_name+"_dailyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
										+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE
										+OracleConstants.COMMA+project_sequence);
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
												.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,monthly_flag,month_run_day,job_schedule_time,project_id")
												.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE
														+OracleConstants.COMMA+project_sequence);
										System.out.println(insertQuery);
										statement.execute(insertQuery); 
									}

								}
								else {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,monthly_flag,month_run_day,job_schedule_time,project_id")
											.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE
													+OracleConstants.COMMA+project_sequence);
									System.out.println(insertQuery);
									statement.execute(insertQuery);
								}
							}




						}else {
							if(minutes.contains(",")) {
								for(String minute:minutes.split(",")) {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,monthly_flag,month_run_day,job_schedule_time,project_id")
											.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE
													+OracleConstants.COMMA+project_sequence);
									System.out.println(insertQuery);
									statement.execute(insertQuery); 
								}

							} 			else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,monthly_flag,month_run_day,job_schedule_time,project_id")
										.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+date+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE
												+OracleConstants.COMMA+project_sequence);
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
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,monthly_flag,month_run_day,job_schedule_time,project_id")
											.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE
													+OracleConstants.COMMA+project_sequence);
									System.out.println(insertQuery);
									statement.execute(insertQuery); 
								}

							}
							else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,monthly_flag,month_run_day,job_schedule_time,project_id")
										.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE
												+OracleConstants.COMMA+project_sequence);
								System.out.println(insertQuery);
								statement.execute(insertQuery);
							}
						}




					}else {
						if(minutes.contains(",")) {
							for(String minute:minutes.split(",")) {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
										.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE
												+OracleConstants.COMMA+project_sequence);
								System.out.println(insertQuery);
								statement.execute(insertQuery); 
							}

						}else {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
									.replace("{$data}",OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+"_monthlyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+dates+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE
											+OracleConstants.COMMA+project_sequence);
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
												.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
												.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
														+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE
														+OracleConstants.COMMA+project_sequence);
										statement.execute(insertQuery);
									}
								}else {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
											.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weekExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE
													+OracleConstants.COMMA+project_sequence);
									statement.execute(insertQuery);
								}
							}
						}else {

							if(minutes.contains(",")) {
								for(String minute:minutes.split(",")) {
									insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
											.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE
													+OracleConstants.COMMA+project_sequence);
									statement.execute(insertQuery);
								}
							}else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
										.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+day+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE
												+OracleConstants.COMMA+project_sequence);
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
											.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
											.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
													+OracleConstants.QUOTE+hour+":"+minute+":00"+OracleConstants.QUOTE
													+OracleConstants.COMMA+project_sequence);
									statement.execute(insertQuery);
								}
							}else {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
										.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hour+":"+minutes+":00"+OracleConstants.QUOTE
												+OracleConstants.COMMA+project_sequence);
								statement.execute(insertQuery);
							}
						}
					}else {

						if(minutes.contains(",")) {
							for(String minute:minutes.split(",")) {
								insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
										.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
										.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
												+OracleConstants.QUOTE+hours+":"+minute+":00"+OracleConstants.QUOTE
												+OracleConstants.COMMA+project_sequence);
								statement.execute(insertQuery);
							}
						}else {
							insertQuery=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.SCHEDULETABLE)
									.replace("{$columns}", "job_id,job_name,batch_id,command,argument_1,daily_flag,job_schedule_time,project_id")
									.replace("{$data}",OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+"_weeklyExtract"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+SchedulerConstants.read_script+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+feed_name+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+"Y"+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+daysOfWeek+OracleConstants.QUOTE+OracleConstants.COMMA
											+OracleConstants.QUOTE+hours+":"+minutes+":00"+OracleConstants.QUOTE
											+OracleConstants.COMMA+project_sequence);
							statement.execute(insertQuery);
						}
					}
				}
			}
		}catch(SQLException e) {
			return e.getMessage();
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
		String updateExtractionMaster="update "+OracleConstants.FEEDTABLE+" set file_list='"+fileListStr+"' where src_sys_id="+hdfsDto.getSrc_sys_id();
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

	@Override
	public String insertHivePropagateMetadata(Connection conn, HiveDbMetadataDto hivedbDto) throws SQLException {
		StringBuffer dbList=new StringBuffer();
		String insertHiveMaster="";
		String sequence="";
		for(String hivedb:hivedbDto.getHiveDbList()) {

			insertHiveMaster=OracleConstants.INSERTQUERY.replace("{$table}", OracleConstants.DBPROPOGATIONTABLE)
					.replace("{$columns}","feed_id,db_name,created_by" )
					.replace("{$data}",hivedbDto.getFeed_id() +OracleConstants.COMMA
							+OracleConstants.QUOTE+hivedb+OracleConstants.QUOTE+OracleConstants.COMMA
							+"(select user_sequence from JUNIPER_USER_MASTER where user_id='"+hivedbDto.getJuniper_user()+"')");
			try {	
				Statement statement = conn.createStatement();
				//System.out.println(insertHiveMaster);
				statement.executeUpdate(insertHiveMaster);
				String query=OracleConstants.GETSEQUENCEID.replace("${tableName}", OracleConstants.DBPROPOGATIONTABLE).replace("${columnName}", "HDB_ID");
				System.out.println("query is "+query);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					rs=statement.executeQuery(OracleConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()) {
						rs.next();
						dbList.append(rs.getString(1)+",");
					}
				}
			}catch (SQLException e) {
				e.printStackTrace();
				return e.getMessage();
			}finally {
				conn.close();
			}
		}
		//dbList.setLength(dbList.length()-1);
		//String dbListStr=dbList.toString();
		//String updateExtractionMaster="update "+OracleConstants.FEEDTABLE+" set table_list='"+dbListStr+"' where feed_sequence="+hivedbDto.getFeed_id();
	
			return "Success";

		
	}

	@Override
	public HiveDbMetadataDto getHivePropagateInfoObject(Connection conn, String dbList) throws SQLException {

		HiveDbMetadataDto hiveInfoDto= new HiveDbMetadataDto();
		ArrayList<String> dbPaths=new ArrayList<String>();
		String[] dbIds=dbList.split(",");
		try {
			for(String dbId:dbIds) {
				String query="select db_name from "+OracleConstants.DBPROPOGATIONTABLE+" where hdb_id="+dbId;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {
					rs.next();
					dbPaths.add(rs.getString(1));
				}
			}
			hiveInfoDto.setHiveDbList(dbPaths);
		}catch(SQLException e) {
			e.printStackTrace();
		}finally {
			conn.close();
		}
		return hiveInfoDto;
	}
}








