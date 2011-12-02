/**
 * 
 */
package com.taobao.top.analysis.node.event;

import com.taobao.top.analysis.node.job.JobTaskResult;

/**
 * Slave返回处理后结果的请求事件
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class SendResultsRequestEvent extends MasterNodeEvent{

	/**
	 * 
	 */
	private static final long serialVersionUID = -914311533130990196L;
	
	JobTaskResult jobTaskResult;
	
	public SendResultsRequestEvent(String sequence)
	{
		this.sequence = sequence;
		this.eventCode = MasterEventCode.SEND_RESULT;
	}

	public JobTaskResult getJobTaskResult() {
		return jobTaskResult;
	}

	public void setJobTaskResult(JobTaskResult jobTaskResult) {
		this.jobTaskResult = jobTaskResult;
	}
	

}
