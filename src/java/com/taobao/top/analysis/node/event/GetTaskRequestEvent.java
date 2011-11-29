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
public class GetTaskRequestEvent extends MasterNodeEvent{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3526811359302681716L;
	
	public GetTaskRequestEvent(String sequence)
	{
		this.eventCode = MasterEventCode.GET_TASK;
		this.sequence = sequence;
	}
	
	String jobName;
	
	int requestJobCount;
	
	
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public int getRequestJobCount() {
		return requestJobCount;
	}

	public void setRequestJobCount(int requestJobCount) {
		this.requestJobCount = requestJobCount;
	}

}
