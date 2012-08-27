package com.taobao.top.analysis.node.monitor;


/**
 * JobTask执行统计日志
 * @author sihai
 *
 */
public class JobExecutionLog extends MonitorLog {
	
	/**
	 * 标题常量
	 */
	public static final String TITLE_JOB_NAME = "实例名称";
	public static final String TITLE_JOB_EXECUTED_COUNT = "任务总执行次数";
	public static final String TITLE_JOB_EXECUTED_SUCCEED_COUNT = "任务成功总执行次数";
	public static final String TITLE_JOB_CONSUME_TIME = "实例执行耗时";
	public static final String TITLE_JOB_DATA_SIZE = "实例数据量";
	public static final String TITLE_JOB_TOTAL_LINE = "实例数据行数";
	public static final String TITLE_JOB_ERROR_LINE = "实例错误数据行数";
	public static final String TITLE_JOB_EMPTY_LINE = "实例空数据行数";
	public static final String TITLE_JOB_KEY_COUNT = "实例产生Key数";
	public static final String TITLE_JOB_VALUE_COUNT = "实例产生Value数";
	
	/**
	 * 被执行的job
	 */
	private String jobName;
	
	private long executedCount;			//
	private long executedSucceedCount;	//
	private long analysisConsume;		//		
	private long jobDataSize;			//
	private long totalLine;				//
	private long errorLine;				//
	private long emptyLine;				//
	private long keyCount;				//
	private long valueCount;			//
	
	/**
	 * 构造函数
	 * @param jobTask
	 * @param jobTaskExecuteInfo
	 */
	public JobExecutionLog(String jobName) {
		this.jobName = jobName;
	}
	
	/**
	 * 
	 * @param taskLog
	 */
	public void plus(JobTaskExecutionLog taskLog) {
		executedCount++;
		if(taskLog.isSuccess()) {
			executedSucceedCount++;
		}
		analysisConsume += taskLog.getAnalysisConsume();
		jobDataSize += taskLog.getJobDataSize();
		totalLine += taskLog.getTotalLine();
		errorLine += taskLog.getErrorLine();
		emptyLine += taskLog.getEmptyLine();
		keyCount += taskLog.getKeyCount();
		valueCount += taskLog.getValueCount();
	}

	public static String title() {
		StringBuilder sb = new StringBuilder(TITLE_JOB_NAME);
		sb.append(",");
		sb.append(TITLE_JOB_EXECUTED_COUNT);
		sb.append(",");
		sb.append(TITLE_JOB_EXECUTED_SUCCEED_COUNT);
		sb.append(",");
		sb.append(TITLE_JOB_CONSUME_TIME);
		sb.append(",");
		sb.append(TITLE_JOB_DATA_SIZE);
		sb.append(",");
		sb.append(TITLE_JOB_TOTAL_LINE);
		sb.append(",");
		sb.append(TITLE_JOB_ERROR_LINE);
		sb.append(",");
		sb.append(TITLE_JOB_EMPTY_LINE);
		sb.append(",");
		sb.append(TITLE_JOB_KEY_COUNT);
		sb.append(",");
		sb.append(TITLE_JOB_VALUE_COUNT);
		return sb.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(jobName);
		sb.append(",");
		sb.append(executedCount);
		sb.append(",");
		sb.append(executedSucceedCount);
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
		return sb.toString();
	}
}