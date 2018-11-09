package com.dataextract.fetchdata;

import java.io.File;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportDataCallable implements Callable<String> {

	public static Logger logger = LoggerFactory.getLogger(ExportDataCallable.class);

	private String tblName;

	public  Connection con;
	public  PreparedStatement pSt;

	String serverURL;
	String username;
	String password;
	String strg_location;
	String src_system_id;
	String src_unique_name;
	String runId;

	public ExportDataCallable(String tblName, String connectionUrl, String userName, String password, String src_system_id, String src_unique_name, String targetLocation, String runId ) {

		this.tblName = tblName;
		this.strg_location=targetLocation;
		this.serverURL = connectionUrl;
		this.username = userName;
		this.password = password;
		this.src_system_id=src_system_id;
		this.src_unique_name=src_unique_name;
		this.runId=runId;
	}


	public String call() throws Exception {

		con = getConnection();
		UncheckedCloseable close=null;
		File home = new File(System.getProperty("user.home")+"/"+strg_location+"/"+src_system_id+"-"+src_unique_name+"/"+runId+"/DATA/");

		home.mkdirs();

		try {

			close=UncheckedCloseable.wrap(con);
			pSt = con.prepareStatement("select * from "+this.tblName);
			close=close.nest(pSt);
			con.setAutoCommit(false);
			pSt.setFetchSize(5000);
			ResultSet resultSet = pSt.executeQuery();
			close=close.nest(resultSet);
			Stream<String> buffer = StreamSupport.stream(new Spliterators.AbstractSpliterator<String>(
					Long.MAX_VALUE,Spliterator.ORDERED) {

				@Override
				public boolean tryAdvance(Consumer<? super String> action) {
					try {
						if(!resultSet.next()) return false;
						action.accept(createRecord(resultSet));
						return true;
					} catch(SQLException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
						throw new RuntimeException(ex);
					}
				}

			}, false).onClose(close);

			Files.write(Paths.get(home.getAbsolutePath()+"/"+tblName+".csv"), (Iterable<String>)buffer::iterator);

		} catch(SQLException sqlEx) {
			if(close!=null)
				try { close.close(); } catch(Exception ex) { sqlEx.addSuppressed(ex); }
			throw sqlEx;
		}
		
		
		createFileDetails();
		
		
		return tblName;
	}
	
	private void createFileDetails() throws IOException {
		/*String key=authUtils.fetchKey(GenericConstants.PROJECTID, KmsConstants.LOCATIONID, KmsConstants.KEYRINGID, KmsConstants.CRYPTOKEYID, serviceAccountName);
		Credentials userCredential=authUtils.getCredentials(key);
		Storage storage=authUtils.getStorageService(userCredential);
		String bucketName = bucket;
	    String fileDetailLocation = "mstr_file_dtls"+".csv";
		BlobId file_blob_id = BlobId.of(bucketName, Integer.toString(src_sys_id)+"-"+src_unique_name+"/"+date+"/"+runId+"/METADATA/"+fileDetailLocation);*/
		StringBuffer filebuffer = new StringBuffer();
		filebuffer.append("\n");
		//for(Map<String,String> tbl:tableInfo) {
				filebuffer.append(src_unique_name+","+tblName+","+tblName+","+src_system_id+"_samplefile"+","+"D"+","+"COMMA"+","+tblName+","+","+"0"+","+"0"+","+"0"+","+"0"+","+"SQL"+","+"I"+"\n");
		//}
		File home =new File(System.getProperty("user.home")+"/"+strg_location+"/"+src_system_id+"-"+src_unique_name+"/"+runId+"/METADATA/");
		Files.write(Paths.get(home.getAbsolutePath()+"/"+"mstr_file_dtls.csv"), filebuffer.toString().getBytes());
		
	}
	
	private String createRecord(ResultSet resultSet) throws SQLException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		StringBuffer buffer = new StringBuffer();
		int colCount = resultSet.getMetaData().getColumnCount();
		if(!(resultSet.isAfterLast()) && resultSet.next()) {
			for(int i=1; i<=colCount; i++) {
				String columnName = resultSet.getMetaData().getColumnTypeName(i);
				String getType = StringUtils.capitalize(columnName.toLowerCase());
				Method methodName = getMethodName(resultSet,getType);
				Object result = methodName.invoke(resultSet, i);
				if(result == null || result.toString().equalsIgnoreCase("null")){
					result =  "";
				}
				if (result instanceof String) {
					result = ((String) result).trim().replaceAll(",", "\\\\,").replace("\n", "").replace("\r", "");
				}
				buffer.append(result+",");
			}
			buffer.setLength(buffer.length()-1);
			buffer.append("\n");
		}
		return buffer.toString();
	}

	private static Method getMethodName(ResultSet rs, String getType) {
		Method method = null;
		String getTeradataType;
		if(getType.equalsIgnoreCase("Decimal") || getType.equalsIgnoreCase("Numeric") || getType.equalsIgnoreCase("Number"))
			getTeradataType="BigDecimal";
		else if(getType.equalsIgnoreCase("Char") || getType.equalsIgnoreCase("Varchar2"))
			getTeradataType="String";
		else if(getType.equalsIgnoreCase("Integer"))
			getTeradataType="Int";
		else if(getType.equalsIgnoreCase("Char"))
			getTeradataType="String";
		else 
			getTeradataType=getType;

		try {
			method = ResultSet.class.getMethod("get"+getTeradataType,int.class);
		} catch (SecurityException e) { e.printStackTrace(); }
		catch (NoSuchMethodException e) { e.printStackTrace(); }
		return method;
	}

	private Connection getConnection() {
		try {
			if(con == null || con.isClosed()) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
				con = DriverManager.getConnection(serverURL, username, password);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}
}
