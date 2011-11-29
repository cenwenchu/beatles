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
public enum SlaveEventCode {
	
	GET_TASK_RESP("getTask"),
	SEND_RESULT_RESP("sendResult");	
	
	String v;
	
	SlaveEventCode(String value)
	{
		this.v = value;
	}
	
	@Override
	public String toString() {
		return v;
	}

}
