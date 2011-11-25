/**
 * 
 */
package com.taobao.top.analysis.job;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.top.analysis.config.JobConfig;
import com.taobao.top.analysis.statistics.data.Rule;

/**
 * 
 * 任务的真实需要执行的实例，每个计算节点根据描述的输入获取数据，
 * 根据规则来分析数据，最后返回结果到输出
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public class JobTask implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4539861249196752195L;
	
	private String taskId;
	private Rule statisticsRule;
	private String input;
	private String output;
	private String splitRegex;
	private String inputEncoding;
	private String outputEncoding;
	private TaskStatus status;
	
	/**
	 * 任务创建时间
	 */
	private long creatTime;
	/**
	 * 任务开始时间
	 */
	private long startTime;
	/**
	 * 任务结束时间
	 */
	private long endTime;
	
	/**
	 * 处理后的结果池，key是entry的id， value是Map(key是entry定义的key组合,value是统计后的结果)
	 * 采用线程不安全，只有单线程操作此结果集
	 */
	private Map<String, Map<String, Object>> results;

	/**
	 * 该job回收计数
	 */
	private AtomicInteger recycleCounter;
	
	private TaskExecuteInfo taskExecuteInfo;
	
	public JobTask(JobConfig jobConfig)
	{
		input = jobConfig.getInput();
		output = jobConfig.getOutput();
		splitRegex = jobConfig.getSplitRegex();
		inputEncoding = jobConfig.getInputEncoding();
		outputEncoding = jobConfig.getOutputEncoding();
		status = TaskStatus.UNDO;
		creatTime = System.currentTimeMillis();
		recycleCounter= new AtomicInteger(0);
		taskExecuteInfo = new TaskExecuteInfo();
		results = new java.util.HashMap<String, Map<String, Object>>();
	}
		
	
	public String getOutputEncoding() {
		return outputEncoding;
	}

	public void setOutputEncoding(String outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public TaskExecuteInfo getTaskExecuteInfo() {
		return taskExecuteInfo;
	}

	public void setTaskExecuteInfo(TaskExecuteInfo taskExecuteInfo) {
		this.taskExecuteInfo = taskExecuteInfo;
	}
	public TaskStatus getStatus() {
		return status;
	}
	public void setStatus(TaskStatus status) {
		this.status = status;
	}
	public long getCreatTime() {
		return creatTime;
	}
	public void setCreatTime(long creatTime) {
		this.creatTime = creatTime;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public Map<String, Map<String, Object>> getResults() {
		return results;
	}
	public void setResults(Map<String, Map<String, Object>> results) {
		this.results = results;
	}
	public AtomicInteger getRecycleCounter() {
		return recycleCounter;
	}
	public void setRecycleCounter(AtomicInteger recycleCounter) {
		this.recycleCounter = recycleCounter;
	}
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public Rule getStatisticsRule() {
		return statisticsRule;
	}
	public void setStatisticsRule(Rule statisticsRule) {
		this.statisticsRule = statisticsRule;
	}
	public String getInput() {
		return input;
	}
	public void setInput(String input) {
		this.input = input;
	}
	public String getOutput() {
		return output;
	}
	public void setOutput(String output) {
		this.output = output;
	}
	public String getSplitRegex() {
		return splitRegex;
	}
	public void setSplitRegex(String splitRegex) {
		this.splitRegex = splitRegex;
	}
	public String getInputEncoding() {
		return inputEncoding;
	}
	public void setInputEncoding(String inputEncoding) {
		this.inputEncoding = inputEncoding;
	}

}
