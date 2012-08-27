/**
 * 
 */
package com.taobao.top.analysis.node.event;

/**
 * 服务端事件消息码
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
	LOAD_BACKUP_DATA("loadBackupData"),
	LOAD_DATA_TO_TMP("loadDataToTmp"),
	CLEAR_DATA("clearData"),
	SUSPEND("suspend"),
	AWAKE("awake"),
	RESETSERVER("resetServer"),
	SEND_MONITOR_INFO("sendMonitorInfo");
	
	
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
