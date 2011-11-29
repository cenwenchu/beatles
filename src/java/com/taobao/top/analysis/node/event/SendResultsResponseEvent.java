package com.taobao.top.analysis.node.event;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class SendResultsResponseEvent extends SlaveNodeEvent{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8406760053051348957L;
	
	public SendResultsResponseEvent(String sequence)
	{
		this.sequence = sequence;
		this.eventCode = SlaveEventCode.SEND_RESULT_RESP;
	}
	
	String response;

	public String getResponse() {
		return response;
	}

	public void setResponse(String response) {
		this.response = response;
	}

}
