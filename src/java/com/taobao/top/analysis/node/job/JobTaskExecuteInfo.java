/**
 * 
 */
package com.taobao.top.analysis.node.job;

import java.io.Serializable;

/**
 * 任务执行时必要的执行信息纪录，用于监控和状态报告
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class JobTaskExecuteInfo implements Serializable {

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
	private String taskId;
	
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
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


    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        sb.append("taskId:").append(taskId).append(",analysisConsume:").append(analysisConsume).append(",jobDataSize:")
            .append(jobDataSize).append(",totalLine:").append(totalLine).append(",errorLine:").append(errorLine)
            .append(",emptyLine:").append(emptyLine).append(",success:").append(success).append(")");
        return sb.toString();
    }
    /**
     * @return the taskId
     */
    public String getTaskId() {
        return taskId;
    }
    /**
     * @param taskId the taskId to set
     */
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

}
