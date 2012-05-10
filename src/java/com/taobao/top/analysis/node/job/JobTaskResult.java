/**
 * 
 */
package com.taobao.top.analysis.node.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.util.ReportUtil;

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
	private Map<String,JobTaskExecuteInfo> taskExecuteInfos;
	
	/**
	 * 处理后的结果池所对应的key
	 */
	private String resultKey;
	
	/**
	 * 所属job的名称
	 */
	private String jobName;
	
	/**
	 * slave的繁忙率，计算总时间/slave运行时间
	 */
	private float efficiency;
	
	private String slaveIp = ReportUtil.getIp();

	public JobTaskResult()
	{
		results = new HashMap<String, Map<String, Object>>();
		taskIds = new ArrayList<String>();
		efficiency = 0;
		taskExecuteInfos = new HashMap<String,JobTaskExecuteInfo>();
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
		clone.efficiency = efficiency;
		
		return clone;
	}
	
	public String getSlaveIp() {
		return slaveIp;
	}

	public void setSlaveIp(String slaveIp) {
		this.slaveIp = slaveIp;
	}

	public float getEfficiency() {
		return efficiency;
	}

	public void setEfficiency(float efficiency) {
		this.efficiency = efficiency;
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
		if (taskExecuteInfo != null)
			taskExecuteInfos.put(taskExecuteInfo.getTaskId(),taskExecuteInfo);
	}
	
	public void addTaskIds(List<String> taskIds)
	{
		this.taskIds.addAll(taskIds);
	}
	
	public void addTaskExecuteInfos(Map<String,JobTaskExecuteInfo> taskExecuteInfos)
	{
		if (taskExecuteInfos != null && taskExecuteInfos.size() > 0)
			this.taskExecuteInfos.putAll(taskExecuteInfos);
	}
	
	public List<String> getTaskIds() {
		return taskIds;
	}

	public void setTaskIds(List<String> taskIds) {
		this.taskIds = taskIds;
	}

	public Map<String,JobTaskExecuteInfo> getTaskExecuteInfos() {
		return taskExecuteInfos;
	}

	public void setTaskExecuteInfos(Map<String,JobTaskExecuteInfo> taskExecuteInfos) {
		this.taskExecuteInfos = taskExecuteInfos;
	}

	public Map<String, Map<String, Object>> getResults() {
		return results;
	}
	public void setResults(Map<String, Map<String, Object>> results) {
		this.results = results;
	}


    public String toString() {
        StringBuilder sb = new StringBuilder("jobTaskResult:[");

        for (JobTaskExecuteInfo jobTaskExecuteInfo : taskExecuteInfos.values()) {
            sb.append(jobTaskExecuteInfo.toString()).append(";");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * @return the resultKey
     */
    public String getResultKey() {
        return resultKey;
    }

    /**
     * @param resultKey the resultKey to set
     */
    public void setResultKey(String resultKey) {
        this.resultKey = resultKey;
    }

    /**
     * @return the jobName
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * @param jobName the jobName to set
     */
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

}
