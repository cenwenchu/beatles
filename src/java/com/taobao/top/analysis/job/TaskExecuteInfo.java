/**
 * 
 */
package com.taobao.top.analysis.job;

import java.io.Serializable;

/**
 * 任务执行时必要的执行信息纪录，用于监控和状态报告
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class TaskExecuteInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5793378193292594458L;
	
	private long analysisConsume;
	private long jobDataSize;
	private long totalLine;
	private long errorLine;
	private long emptyLine;
	private boolean success;
	
	/**
	 * 工作者IP
	 */
	private String workerIp;
	
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
	public String getWorkerIp() {
		return workerIp;
	}
	public void setWorkerIp(String workerIp) {
		this.workerIp = workerIp;
	}
	public long getAnalysisConsume() {
		return analysisConsume;
	}
	public void setAnalysisConsume(long analysisConsume) {
		this.analysisConsume = analysisConsume;
	}
	public long getJobDataSize() {
		return jobDataSize;
	}
	public void setJobDataSize(long jobDataSize) {
		this.jobDataSize = jobDataSize;
	}
	public long getTotalLine() {
		return totalLine;
	}
	public void setTotalLine(long totalLine) {
		this.totalLine = totalLine;
	}
	public long getErrorLine() {
		return errorLine;
	}
	public void setErrorLine(long errorLine) {
		this.errorLine = errorLine;
	}
	public long getEmptyLine() {
		return emptyLine;
	}
	public void setEmptyLine(long emptyLine) {
		this.emptyLine = emptyLine;
	}

}
