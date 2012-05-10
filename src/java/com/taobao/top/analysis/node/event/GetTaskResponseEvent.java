/**
 * 
 */
package com.taobao.top.analysis.node.event;

import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;

/**
 * 获取任务请求的响应事件
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class GetTaskResponseEvent extends SlaveNodeEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6410998551027439173L;
	
	public GetTaskResponseEvent(String sequence)
	{
		this.sequence = sequence;
		this.eventCode = SlaveEventCode.GET_TASK_RESP;
	}
	
	/**
	 * 处理后返回的任务结果
	 */
	List<JobTask> jobTasks;

	public List<JobTask> getJobTasks() {
		return jobTasks;
	}

	public void setJobTasks(List<JobTask> jobTasks) {
		this.jobTasks = jobTasks;
	}
	
	

}
