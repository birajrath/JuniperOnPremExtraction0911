package com.dataextract.dto;

public class ConnectionDto {

	
	String conn_name;
	String conn_type;
	String hostName;
	String port;
	String userName;
	String password;
	String dbName;
	String serviceName;
	byte[] encr_key;
	byte[] encrypted_password;
	int drive_id;
	int connId;
	String system;
	String project;
	String juniper_user;
	int sys_seq;
	int proj_seq;
	String knox_gateway;
	String trust_store_path;
	String trust_store_password;
	
	
	
	
	
	public byte[] getEncr_key() {
		return encr_key;
	}

	public void setEncr_key(byte[] encr_key) {
		this.encr_key = encr_key;
	}

	public byte[] getEncrypted_password() {
		return encrypted_password;
	}

	public void setEncrypted_password(byte[] encrypted_password) {
		this.encrypted_password = encrypted_password;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}
	
	public String getJuniper_user() {
		return juniper_user;
	}
	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}
	public int getSys_seq() {
		return sys_seq;
	}
	public void setSys_seq(int sys_seq) {
		this.sys_seq = sys_seq;
	}
	public int getProj_seq() {
		return proj_seq;
	}
	public void setProj_seq(int proj_seq) {
		this.proj_seq = proj_seq;
	}
	public int getDrive_id() {
		return drive_id;
	}
	public void setDrive_id(int drive_id) {
		this.drive_id = drive_id;
	}
	
	public int getConnId() {
		return connId;
	}
	public void setConnId(int connId) {
		this.connId = connId;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	public String getServiceName() {
		return serviceName;
	}
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	
	
	
	
	public String getConn_name() {
		return conn_name;
	}
	public void setConn_name(String conn_name) {
		this.conn_name = conn_name;
	}
	public String getConn_type() {
		return conn_type;
	}
	public void setConn_type(String conn_type) {
		this.conn_type = conn_type;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getSystem() {
		return system;
	}
	public void setSystem(String system) {
		this.system = system;
	}

	public String getKnox_gateway() {
		return knox_gateway;
	}

	public void setKnox_gateway(String knox_gateway) {
		this.knox_gateway = knox_gateway;
	}

	public String getTrust_store_path() {
		return trust_store_path;
	}

	public void setTrust_store_path(String trust_store_path) {
		this.trust_store_path = trust_store_path;
	}

	public String getTrust_store_password() {
		return trust_store_password;
	}

	public void setTrust_store_password(String trust_store_password) {
		this.trust_store_password = trust_store_password;
	}

	
	
}
