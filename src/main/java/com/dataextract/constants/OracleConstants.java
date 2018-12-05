package com.dataextract.constants;

public class OracleConstants {

	public final static String ORACLE_DRIVER="oracle.jdbc.driver.OracleDriver";
	public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	public final static String ORACLE_IP_PORT_SID="35.227.48.30:1521:orcl";
	public final static String ORACLE_USER_NAME="juniper_admin";
	public final static String ORACLE_JDBC_URL="jdbc:oracle:thin:@"+ORACLE_IP_PORT_SID+"";
	public static final String ORACLE_PASSWORD = "Infy123##";
	public static final String SYSTEMTABLE = "JUNIPER_SYSTEM_MASTER";
	public static final String PROJECTTABLE = "JUNIPER_PROJECT_MASTER";
	public static final String CONNECTIONTABLE="JUNIPER_EXT_SRC_CONN_MASTER";
	public static final String CONNECTIONTABLEKEY="SRC_CONN_SEQUENCE";
	public static final String TARGETTABLEKEY = "TARGET_CONN_SEQUENCE";
	public static final String FEEDTABLEKEY = "FEED_SEQUENCE";
	public static final String TABLEKEY = "TABLE_SEQUENCE";
	public static final String EXTRACTSTATUSTABLE = "JUNIPER_EXT_EXTRACTION_STATUS";
	public static final String TABLEDETAILSTABLE="JUNIPER_EXT_TABLE_MASTER";
	public static final String FEEDTABLE = "JUNIPER_EXT_FEED_MASTER";
	public static final String SCHEDULETABLE = "JUNIPER_SCH_MASTER_JOB_DETAIL";
	public static final String KEYTABLE = "JUNIPER_EXT_KEY_MASTER";
	public static final String INSERTQUERY = "insert into {$table}({$columns}) values({$data})";

	public static final String QUOTE = "'";
	public static final String COMMA = ",";
	public static final String TAREGTTABLE = "JUNIPER_EXT_TARGET_CONN_MASTER";
	public static final String GETSEQUENCEID="Select  DATA_DEFAULT from USER_TAB_COLUMNS where TABLE_NAME = '${tableName}' and COLUMN_NAME='${columnName}'";
	public static final String GETLASTROWID="SELECT ${id}.currval from dual";
	public static final String FILEDETAILSTABLE = "JUNIPER_EXT_FILE_MASTER";
	public static final String FIELDDETAILSTABLE="JUNIPER_EXT_FIELD_MASTER";
	public static final String DRIVETABLE = "JUNIPER_EXT_DRIVE_MASTER";
	public static final String HDFSDETAILSTABLE = "JUNIPER_HDFS_MASTER";
	public static final String NIFISTATUSTABLE = "JUNIPER_EXT_NIFI_STATUS";
	public static final String GCPTABLE = "JUNIPER_EXT_GCP_MASTER";
	public static final String DBPROPOGATIONTABLE="JUNIPER_PROGT_DB_LIST_MASTER";
	
	
	
	
	
	
	
	
	
	
}
