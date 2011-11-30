/**
 * 
 */
package com.taobao.top.analysis.node.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
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
	
	private List<String> taskIds;
	
	/**
	 * 处理后的结果池，key是entry的id， value是Map(key是entry定义的key组合,value是统计后的结果)
	 * 采用线程不安全，只有单线程操作此结果集
	 */
	private Map<String, Map<String, Object>> results;

	private List<JobTaskExecuteInfo> taskExecuteInfos;
	
	public JobTaskResult()
	{
		results = new java.util.HashMap<String, Map<String, Object>>();
		taskIds = new ArrayList<String>();
		taskExecuteInfos = new ArrayList<JobTaskExecuteInfo>();
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
