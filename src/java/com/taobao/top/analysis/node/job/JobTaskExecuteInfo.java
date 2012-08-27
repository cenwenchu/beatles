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
	private long keyCount;
	private long valueCount;
	private boolean success;
	private String taskId;
	private long fileBegin;
	private long fileLength;
	private long timestamp;
	
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
	public long getKeyCount() {
		return keyCount;
	}
	public void setKeyCount(long keyCount) {
		this.keyCount = keyCount;
	}
	public long getValueCount() {
		return valueCount;
	}
	public void setValueCount(long valueCount) {
		this.valueCount = valueCount;
	}
	
	public void incKeyCount(int count) {
		keyCount += count;
	}
	
	public void incValueCount(int count) {
		valueCount += count;
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
    /**
     * @return the fileBegin
     */
    public long getFileBegin() {
        return fileBegin;
    }
    /**
     * @param fileBegin the fileBegin to set
     */
    public void setFileBegin(long fileBegin) {
        this.fileBegin = fileBegin;
    }
    /**
     * @return the fileLength
     */
    public long getFileLength() {
        return fileLength;
    }
    /**
     * @param fileLength the fileLength to set
     */
    public void setFileLength(long fileLength) {
        this.fileLength = fileLength;
    }
    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

}
