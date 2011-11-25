package com.taobao.top.analysis.job;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.config.JobConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.statistics.data.Rule;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public class Job {
	
	String jobName;
	JobConfig jobConfig;
	Rule statisticsRule;
	List<JobTask> jobTasks;
	int taskCount = 0;
	
	public void generateJobTasks() throws AnalysisException
	{
		if (jobConfig == null)
			throw new AnalysisException("generateJobTasks error, jobConfig is null.");
		
		if (jobConfig.getInputParams() == null)
		{
			JobTask jobTask = new JobTask(jobConfig);
			jobTask.setStatisticsRule(statisticsRule);
			jobTask.setTaskId(jobName + taskCount);
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
				jobTask.setTaskId(jobName + taskCount);
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

}
