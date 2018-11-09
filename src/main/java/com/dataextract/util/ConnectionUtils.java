package com.dataextract.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.dataextract.constants.OracleConstants;

public class ConnectionUtils {
	static Connection connection = null;
	
	 public static Connection connectToMySql(String ip,String port,String db,String user,String password) throws SQLException {

		 
		
			
			 try {
					
					if (connection == null || connection.isClosed()) {
						Class.forName("com.mysql.cj.jdbc.Driver");
						
						String jdbc = "jdbc:mysql://"+ip+":"+port+"/"+db;
						System.out.println(jdbc);
						connection = DriverManager.getConnection(jdbc, user, password);
					}
			 } catch (ClassNotFoundException | SQLException e) {
				   e.printStackTrace();
					throw new SQLException("Exception occured while connecting to mysql");
				}
		 
		
		 	System.out.println("connection succeeded");
			return connection;
		 
	 }	 
	 
	 public static Connection connectToOracle(String ipPortDb,String user,String password) throws SQLException {

		 
			
			
		 try {
				
				if (connection == null || connection.isClosed()) {
					Class.forName(OracleConstants.ORACLE_DRIVER);
					
					String jdbc = "jdbc:oracle:thin:@"+ipPortDb;
					System.out.println(jdbc);
					connection = DriverManager.getConnection(jdbc, user, password);
				}
		 } catch (ClassNotFoundException | SQLException e) {
			   e.printStackTrace();
				throw new SQLException("Exception occured while connecting to mysql");
			}
	 
	
	 	System.out.println("connection succeeded");
		return connection;
	 
 }	 

}
