/**
 * 
 */
package com.taobao.top.analysis.node.event;

import java.util.concurrent.CountDownLatch;

import com.taobao.top.analysis.util.TimeOutEvent;



/**
 * 服务端的事件定义
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class MasterNodeEvent implements INodeEvent,TimeOutEvent{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7679801760752260905L;
	
	MasterEventCode eventCode;
	
	String sequence;
	
	private CountDownLatch resultReadyFlag = new CountDownLatch(1);
	
	private Object response;
	
	protected long eventCreateTime;
	

	public Object getResponse() {
		return response;
	}

	public void setResponse(Object response) {
		this.response = response;
	}

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String sequence) {
		this.sequence = sequence;
	}

	public MasterEventCode getEventCode() {
		return eventCode;
	}

	public void setEventCode(MasterEventCode eventCode) {
		this.eventCode = eventCode;
	}

	public CountDownLatch getResultReadyFlag() {
		return resultReadyFlag;
	}

	public void setResultReadyFlag(CountDownLatch resultReadyFlag) {
		this.resultReadyFlag = resultReadyFlag;
	}

	@Override
	public long getEventCreateTime() {
		return eventCreateTime;
	}

	@Override
	public void setEventCreateTime(long eventCreateTime) {
		this.eventCreateTime = eventCreateTime;
	}
	

}
