/**
 * 
 */
package com.taobao.top.analysis.node.event;


/**
 * 获取任务请求事件
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
		this.eventCreateTime = System.currentTimeMillis();
		this.eventCode = MasterEventCode.GET_TASK;
		this.sequence = sequence;
	}
	
	/**
	 * 可以只指定获取某一个任务的Task，不设置就获取所有的Task
	 */
	String jobName;
	
	/**
	 * 一次请求需要要多少任务，如果服务端不够，就返回小与此值个数的任务
	 */
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
