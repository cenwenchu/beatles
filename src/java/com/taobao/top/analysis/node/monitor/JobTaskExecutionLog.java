package com.taobao.top.analysis.node.monitor;

import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;

/**
 * JobTask执行统计日志
 * @author sihai
 *
 */
public class JobTaskExecutionLog extends MonitorLog {
	
	/**
	 * 标题常量
	 */
	public static final String TITLE_JOB_NAME = "实例名称";
	public static final String TITLE_TASK_ID = "任务标识";
	public static final String TITLE_TASK_CONSUME_TIME = "任务执行耗时";
	public static final String TITLE_TASK_DATA_SIZE = "任务数据量";
	public static final String TITLE_TASK_TOTAL_LINE = "任务数据行数";
	public static final String TITLE_TASK_ERROR_LINE = "任务错误数据行数";
	public static final String TITLE_TASK_EMPTY_LINE = "任务空数据行数";
	public static final String TITLE_TASK_KEY_COUNT = "任务产生Key数";
	public static final String TITLE_TASK_VALUE_COUNT = "任务产生Value数";
	public static final String TITLE_TASK_SUCCEED = "任务执行是否成功";
	
	private String jobName;			// 被执行的jobName
	private String taskId;			// 被执行的任务id
	
	private long analysisConsume;	// 任务数据分析执行消耗时间, 单位毫秒
	private long jobDataSize;		// 任务分析的数据大小, 单位byte
	private long totalLine;			// 任务数据行数
	private long errorLine;			// 任务错误数据行数
	private long emptyLine;			// 任务空数据行数
	private long keyCount;			// 任务map key 数量
	private long valueCount;		// 任务map value 数量
	private boolean success;		// 任务是否执行成功
	private long fileBegin;			// 
	private long fileLength;		// 
	
	/**
	 * 构造函数
	 * @param jobTask
	 * @param jobTaskExecuteInfo
	 */
	public JobTaskExecutionLog(String jobName, JobTaskExecuteInfo jobTaskExecuteInfo) {
		this.jobName = jobName;
		this.taskId = jobTaskExecuteInfo.getTaskId();
		this.analysisConsume = jobTaskExecuteInfo.getAnalysisConsume();
		this.jobDataSize = jobTaskExecuteInfo.getJobDataSize();
		this.totalLine = jobTaskExecuteInfo.getTotalLine();
		this.errorLine = jobTaskExecuteInfo.getErrorLine();
		this.emptyLine = jobTaskExecuteInfo.getEmptyLine();
		this.keyCount = jobTaskExecuteInfo.getKeyCount();
		this.valueCount = jobTaskExecuteInfo.getValueCount();
		this.success = jobTaskExecuteInfo.isSuccess();
		this.fileBegin = jobTaskExecuteInfo.getFileBegin();
		this.fileLength = jobTaskExecuteInfo.getFileLength();
	}

	public static String title() {
		StringBuilder sb = new StringBuilder(TITLE_JOB_NAME);
		sb.append(",");
		sb.append(TITLE_TASK_ID);
		sb.append(",");
		sb.append(TITLE_TASK_CONSUME_TIME);
		sb.append(",");
		sb.append(TITLE_TASK_DATA_SIZE);
		sb.append(",");
		sb.append(TITLE_TASK_TOTAL_LINE);
		sb.append(",");
		sb.append(TITLE_TASK_ERROR_LINE);
		sb.append(",");
		sb.append(TITLE_TASK_EMPTY_LINE);
		sb.append(",");
		sb.append(TITLE_TASK_KEY_COUNT);
		sb.append(",");
		sb.append(TITLE_TASK_VALUE_COUNT);
		sb.append(",");
		sb.append(TITLE_TASK_SUCCEED);
		return sb.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(jobName);
		sb.append(",");
		sb.append(taskId);
		sb.append(",");
		sb.append(analysisConsume);
		sb.append(",");
		sb.append(jobDataSize);
		sb.append(",");
		sb.append(totalLine);
		sb.append(",");
		sb.append(errorLine);
		sb.append(",");
		sb.append(emptyLine);
		sb.append(",");
		sb.append(keyCount);
		sb.append(",");
		sb.append(valueCount);
		sb.append(",");
		sb.append(success);
		return sb.toString();
	}
	
	public String getJobName() {
		return jobName;
	}
	public String getTaskId() {
		return taskId;
	}
	public long getAnalysisConsume() {
		return analysisConsume;
	}
	public long getJobDataSize() {
		return jobDataSize;
	}
	public long getTotalLine() {
		return totalLine;
	}
	public long getErrorLine() {
		return errorLine;
	}
	public long getEmptyLine() {
		return emptyLine;
	}
	public long getKeyCount() {
		return keyCount;
	}
	public long getValueCount() {
		return valueCount;
	}
	public boolean isSuccess() {
		return success;
	}
	public long getFileBegin() {
		return fileBegin;
	}
	public long getFileLength() {
		return fileLength;
	}
}