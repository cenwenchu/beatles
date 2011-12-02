/**
 * 
 */
package com.taobao.top.analysis.node.event;



/**
 * 服务端的事件定义
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class MasterNodeEvent implements INodeEvent{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7679801760752260905L;
	
	MasterEventCode eventCode;
	
	String sequence;

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

}
