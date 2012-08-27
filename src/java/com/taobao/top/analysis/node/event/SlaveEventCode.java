/**
 * 
 */
package com.taobao.top.analysis.node.event;

/**
 * 客户端消息码
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public enum SlaveEventCode {
	
	GET_TASK_RESP("getTask"),
	SEND_RESULT_RESP("sendResult"),
	SUSPEND("suspend"),
	AWAKE("awake"),
	CHANGE_MASTER("changeMaster"),
	SEND_MONITOR_INFO_RESP("sendMonitorInfo.Response");	
	
	String v;
	
	SlaveEventCode(String value) {
		this.v = value;
	}
	
	@Override
	public String toString() {
		return v;
	}

}
