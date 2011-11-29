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
public class JobManageEvent extends MasterNodeEvent {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7917458266777132001L;
	
	String jobName;
	
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

}
