/**
 * 
 */
package com.taobao.top.analysis.job;

import java.io.Serializable;
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
	
	private String[] taskIds;
	
	/**
	 * 处理后的结果池，key是entry的id， value是Map(key是entry定义的key组合,value是统计后的结果)
	 * 采用线程不安全，只有单线程操作此结果集
	 */
	private Map<String, Map<String, Object>> results;

	private JobTaskExecuteInfo taskExecuteInfo;
	
	public JobTaskResult(String... taskId)
	{
		this.taskIds = taskId;
		taskExecuteInfo = new JobTaskExecuteInfo();
		results = new java.util.HashMap<String, Map<String, Object>>();
	}
	
	public String[] getTaskIds() {
		return taskIds;
	}

	public JobTaskExecuteInfo getTaskExecuteInfo() {
		return taskExecuteInfo;
	}

	public void setTaskExecuteInfo(JobTaskExecuteInfo taskExecuteInfo) {
		this.taskExecuteInfo = taskExecuteInfo;
	}
	
	public Map<String, Map<String, Object>> getResults() {
		return results;
	}
	public void setResults(Map<String, Map<String, Object>> results) {
		this.results = results;
	}

}
