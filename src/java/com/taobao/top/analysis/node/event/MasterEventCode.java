/**
 * 
 */
package com.taobao.top.analysis.node.event;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public enum MasterEventCode {
	
	GET_TASK("getTask"),
	SEND_RESULT("sendResult"),
	RELOAD_JOBS("reloadJobs"),
	EXPORT_DATA("exportData"),
	LOAD_DATA("loadData"),
	CLEAR_DATA("clearData");
	
	
	String v;
	
	MasterEventCode(String value)
	{
		this.v = value;
	}
	
	@Override
	public String toString() {
		return v;
	}

}
