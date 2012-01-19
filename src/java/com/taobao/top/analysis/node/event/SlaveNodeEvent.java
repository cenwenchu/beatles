/**
 * 
 */
package com.taobao.top.analysis.node.event;



/**
 * Slave的事件定义
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class SlaveNodeEvent implements INodeEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2299360116837673453L;
	
	SlaveEventCode eventCode;
	
	String sequence;
	
	protected transient Object channel;
	
	Object attachment;
	

	public Object getAttachment() {
		return attachment;
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String sequence) {
		this.sequence = sequence;
	}

	public SlaveEventCode getEventCode() {
		return eventCode;
	}

	public void setEventCode(SlaveEventCode eventCode) {
		this.eventCode = eventCode;
	}

	public Object getChannel() {
		return channel;
	}

	public void setChannel(Object channel) {
		this.channel = channel;
	}	

}
