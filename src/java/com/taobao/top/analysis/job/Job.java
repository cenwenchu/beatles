package com.taobao.top.analysis.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.JobConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.statistics.data.Rule;
import com.taobao.top.analysis.util.Threshold;

/**
 * 任务结构体，自我描述了数据来源，数据输出，分析规则，包含的子任务
 * 每个子任务可以被提交到集群的单台机器执行，
 * 可以认为就是每个计算节点所处理无差别任务定义。
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public class Job {
	
	private static final Log logger = LogFactory.getLog(Job.class);
	
	String jobName;
	JobConfig jobConfig;
	Rule statisticsRule;
	List<JobTask> jobTasks;
	int taskCount = 0;
	AtomicInteger completedTaskCount;
	AtomicInteger mergedTaskCount;
	long startTime;
	Threshold threshold;
	boolean merged;
	ReentrantReadWriteLock trunckLock;
	
	/**
	 * 处理后的结果池，key是entry的id， value是Map(key是entry定义的key组合,value是统计后的结果)
	 * 采用线程不安全，只有单线程操作此结果集
	 */
	private Map<String, Map<String, Object>> jobResult;
	
	public Job()
	{
		jobTasks = new ArrayList<JobTask>();
		threshold = new Threshold(1000);
		trunckLock = new ReentrantReadWriteLock();
		reset();
	}
	
	
	public ReentrantReadWriteLock getTrunckLock() {
		return trunckLock;
	}


	public void setTrunckLock(ReentrantReadWriteLock trunckLock) {
		this.trunckLock = trunckLock;
	}



	public boolean needMerge()
	{
		return completedTaskCount.get() > mergedTaskCount.get();
	}
	
	public boolean needExport()
	{
		return !merged && mergedTaskCount.get() == taskCount;
	}
	
	
	public void setMerged(boolean merged) {
		this.merged = merged;
	}

	public boolean needReset()
	{
		long consume = System.currentTimeMillis() - startTime;
				
		if ((merged && (consume >= jobConfig.getJobResetTime() * 1000))
				||(consume > jobConfig.getJobResetTime() * 1000 * 2))
			return true;
		
		if (mergedTaskCount.get() < taskCount && consume > jobConfig.getJobResetTime() * 1000)
			if (logger.isWarnEnabled() && threshold.sholdBlock())
				logger.warn("job : " + jobName + " can't complete in time!");
		
		return false;
	}
	
	public void reset()
	{
		for(JobTask task : jobTasks)
		{
			task.setStatus(JobTaskStatus.UNDO);
			task.setCreatTime(System.currentTimeMillis());
			task.getRecycleCounter().set(0);
		}
			
		taskCount = jobTasks.size();
		
		completedTaskCount = new AtomicInteger(0);
		mergedTaskCount = new AtomicInteger(0);
		startTime = System.currentTimeMillis();
		merged = false;
	}

	public Map<String, Map<String, Object>> getJobResult() {
		return jobResult;
	}

	public void setJobResult(Map<String, Map<String, Object>> jobResult) {
		this.jobResult = jobResult;
	}

	public void generateJobTasks() throws AnalysisException
	{
		
		jobTasks.clear();
		
		if (jobConfig == null)
			throw new AnalysisException("generateJobTasks error, jobConfig is null.");
		
		if (jobConfig.getInputParams() == null)
		{
			JobTask jobTask = new JobTask(jobConfig);
			jobTask.setStatisticsRule(statisticsRule);
			jobTask.setTaskId(jobName + "-" + taskCount);
			jobTask.setJobName(jobName);
			taskCount += 1;
			jobTasks.add(jobTask);
		}
		else
		{
			String[] p = StringUtils.split(jobConfig.getInputParams(),":");
			String key = new StringBuilder("$").append(p[0]).append("$").toString();
			
			if (p.length != 2 || jobConfig.getInput().indexOf(key) < 0)
				throw new AnalysisException("inputParams invalidate : " + jobConfig.getInputParams());
			
			String[] params = StringUtils.split(p[1],",");
			
			for(String ps : params)
			{
				JobTask jobTask = new JobTask(jobConfig);
				jobTask.setStatisticsRule(statisticsRule);
				jobTask.setTaskId(jobName + "-" + taskCount);
				jobTask.setJobName(jobName);
				jobTask.setInput(jobConfig.getInput().replace(key, ps));
				taskCount += 1;
				jobTasks.add(jobTask);
			}
			
		}
		
	}
	
	public List<JobTask> getJobTasks() {
		return jobTasks;
	}


	public String getJobName() {
		return jobName;
	}


	public void setJobName(String jobName) {
		this.jobName = jobName;
	}


	public JobConfig getJobConfig() {	
		return jobConfig;
	}

	
	public void setJobConfig(JobConfig jobconfig) {
		this.jobConfig = jobconfig;
	}

	
	public Rule getStatisticsRule() {
		return statisticsRule;
	}

	public void setStatisticsRule(Rule rule) {
		this.statisticsRule = rule;
	}

	public int getTaskCount() {
		return taskCount;
	}

	public void setTaskCount(int taskCount) {
		this.taskCount = taskCount;
	}

	public AtomicInteger getCompletedTaskCount() {
		return completedTaskCount;
	}

	public void setCompletedTaskCount(AtomicInteger completedTaskCount) {
		this.completedTaskCount = completedTaskCount;
	}

	public AtomicInteger getMergedTaskCount() {
		return mergedTaskCount;
	}

	public void setMergedTaskCount(AtomicInteger mergedTaskCount) {
		this.mergedTaskCount = mergedTaskCount;
	}

}
