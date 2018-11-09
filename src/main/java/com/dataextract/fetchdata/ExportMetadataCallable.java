package com.dataextract.fetchdata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportMetadataCallable implements Callable<String> {

	public static Logger logger = LoggerFactory.getLogger(ExportMetadataCallable.class);

	private String tblName;

	public  Connection con;
	public  Statement st;
	public StringBuffer stringBuffer;

	String serverURL;
	String username;
	String password;
	String strg_location;
	String src_system_id;
	String src_unique_name;
	String runId;
	File home;

	public ExportMetadataCallable(String tblName, String connectionUrl, String userName, String password, String src_system_id, String src_unique_name, String targetLocation, String runId ) {

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
		
		StringBuffer buffer = new StringBuffer();
		con = getConnection();
		home = new File(System.getProperty("user.home")+"/"+strg_location+"/"+src_system_id+"-"+src_unique_name+"/"+runId+"/METADATA/");
		home.mkdirs();

		st = con.createStatement();
		System.out.println(username+"-------"+tblName);
		ResultSet resultSet = st.executeQuery("SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS \r\n" + 
				"WHERE  OWNER = '"+username.toUpperCase()+"' AND TABLE_NAME = '"+tblName+"'");
		while(resultSet.next()) {
				buffer.append(resultSet.getString(1)+","+resultSet.getString(2)+"\n");
		}
		
		Files.write(Paths.get(home.getAbsolutePath()+"/"+tblName+".csv"), buffer.toString().getBytes());
		
		return tblName;
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
