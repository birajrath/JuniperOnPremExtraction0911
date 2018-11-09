package com.dataextract.constants;

public class MySQLConstants {

	public static final String MYSQLIP = "35.237.17.136";
	public static final String MYSQLPORT = "3306";
	public static final String DB = "iigs_scheduler_db";
	public static final String USER = "onprime";
	public static final String PASSWORD = "Infy@123";
	public static final String EXTRACTSTATUSTABLE = "extraction_status";
	public static final String TABLEDETAILSTABLE="table_master";
	public static final String EXTRACTIONTABLE = "extraction_master";
	public static final String SOURCESYSTEMTABLE="source_system_master";
	public static final String CONNECTIONTABLE="connection_master";
	public static final String COMPOSERTABLE="composer_master";
	public static final String RESERVOIRTABLE="reservoir_master";
	public static final String SCHEDULETABLE = "iigs_ui_master_job_detail";
	public static final String INSERTQUERY = "insert into {$table}({$columns}) values({$data})";

	public static final String QUOTE = "'";
	public static final String COMMA = ",";
	public static final String TAREGTTABLE = "target_master";

}
