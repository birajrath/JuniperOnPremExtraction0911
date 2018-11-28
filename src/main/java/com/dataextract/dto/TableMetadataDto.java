package com.dataextract.dto;

public class TableMetadataDto {

	String table_name;
	String columns;
	String where_clause;
	String fetch_type;
	String incr_col;
	
	
	
	
	
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getWhere_clause() {
		return where_clause;
	}
	public void setWhere_clause(String where_clause) {
		this.where_clause = where_clause;
	}
	public String getFetch_type() {
		return fetch_type;
	}
	public void setFetch_type(String fetch_type) {
		this.fetch_type = fetch_type;
	}
	public String getIncr_col() {
		return incr_col;
	}
	public void setIncr_col(String incr_col) {
		this.incr_col = incr_col;
	}
	
	
}
