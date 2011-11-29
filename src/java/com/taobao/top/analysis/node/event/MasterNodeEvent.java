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
public class MasterNodeEvent implements INodeEvent{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7679801760752260905L;
	
	MasterEventCode eventCode;
	
	public MasterEventCode getEventCode() {
		return eventCode;
	}

	public void setEventCode(MasterEventCode eventCode) {
		this.eventCode = eventCode;
	}	

}
