/**
 * 
 */
package com.taobao.top.analysis.node.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 任务执行后得到的结果
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class JobTaskResult implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -150184766178723011L;
	
	/**
	 * 支持slave将多个结果合并为一个，包含多个taskid，不过task必须隶属于一个job
	 */
	private List<String> taskIds;
	
	private int jobEpoch;
	
	/**
	 * 处理后的结果池，key是entry的id， value是Map(key是entry定义的key组合,value是统计后的结果)
	 * 采用线程不安全，只有单线程操作此结果集
	 */
	private Map<String, Map<String, Object>> results;

	/**
	 * 一个或者多个执行任务的信息，支持slave将多个结果合并为一个，不过task必须隶属于一个job
	 */
	private List<JobTaskExecuteInfo> taskExecuteInfos;
	
	public JobTaskResult()
	{
		results = new java.util.HashMap<String, Map<String, Object>>();
		taskIds = new ArrayList<String>();
		taskExecuteInfos = new ArrayList<JobTaskExecuteInfo>();
	}
	
	/**
	 * 克隆一个对象，但不包含里面的结果集
	 * @return
	 * @throws CloneNotSupportedException
	 */
	public JobTaskResult cloneWithOutResults() {
		JobTaskResult clone = new JobTaskResult();
		clone.jobEpoch = jobEpoch;
		clone.taskIds = taskIds;
		clone.taskExecuteInfos = taskExecuteInfos;
		
		return clone;
	}
	
	
	public int getJobEpoch() {
		return jobEpoch;
	}


	public void setJobEpoch(int jobEpoch) {
		this.jobEpoch = jobEpoch;
	}


	public void addTaskId(String taskId)
	{
		taskIds.add(taskId);
	}
	
	public void addTaskExecuteInfo(JobTaskExecuteInfo taskExecuteInfo)
	{
		taskExecuteInfos.add(taskExecuteInfo);
	}
	
	public void addTaskIds(List<String> taskIds)
	{
		this.taskIds.addAll(taskIds);
	}
	
	public void addTaskExecuteInfos(List<JobTaskExecuteInfo> taskExecuteInfos)
	{
		this.taskExecuteInfos.addAll(taskExecuteInfos);
	}
	
	public List<String> getTaskIds() {
		return taskIds;
	}

	public void setTaskIds(List<String> taskIds) {
		this.taskIds = taskIds;
	}

	public List<JobTaskExecuteInfo> getTaskExecuteInfos() {
		return taskExecuteInfos;
	}

	public void setTaskExecuteInfos(List<JobTaskExecuteInfo> taskExecuteInfos) {
		this.taskExecuteInfos = taskExecuteInfos;
	}

	public Map<String, Map<String, Object>> getResults() {
		return results;
	}
	public void setResults(Map<String, Map<String, Object>> results) {
		this.results = results;
	}


}
