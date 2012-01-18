/**
 * 
 */
package com.taobao.top.analysis.node.event;


/**
 * 任务管理类型的事件
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class JobManageEvent extends MasterNodeEvent {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7917458266777132001L;
	
	/**
	 * 所涉及的Job的任务管理
	 */
	String jobName;
	
	/**
	 * 可以附加一些参数和内容
	 */
	Object attachment;
	
	public Object getAttachment() {
		return attachment;
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

}
