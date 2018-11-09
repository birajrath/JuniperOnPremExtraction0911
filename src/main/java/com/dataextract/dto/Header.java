/**
 * 
 */
package com.dataextract.dto;

/**
 * @author birajrath
 *
 */
public class Header {

String user;
String service_account;
String reservoir_id;
String event_time;
public String getUser() {
	return user;
}
public void setUser(String user) {
	this.user = user;
}
public String getService_account() {
	return service_account;
}
public void setService_account(String service_account) {
	this.service_account = service_account;
}
public String getReservoir_id() {
	return reservoir_id;
}
public void setReservoir_id(String reservoir_id) {
	this.reservoir_id = reservoir_id;
}
public String getEvent_time() {
	return event_time;
}
public void setEvent_time(String event_time) {
	this.event_time = event_time;
}


}
