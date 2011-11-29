/**
 * 
 */
package com.taobao.top.analysis.node.event;

import com.taobao.top.analysis.job.JobTaskResult;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class JobResponseEvent extends MasterNodeEvent{

	/**
	 * 
	 */
	private static final long serialVersionUID = -914311533130990196L;
	
	JobTaskResult jobTaskResult;

	public JobTaskResult getJobTaskResult() {
		return jobTaskResult;
	}

	public void setJobTaskResult(JobTaskResult jobTaskResult) {
		this.jobTaskResult = jobTaskResult;
	}
	

}
