/**
 * 
 */
package com.taobao.top.analysis.node.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.job.Job;
import com.taobao.top.analysis.job.JobTask;
import com.taobao.top.analysis.job.JobTaskResult;
import com.taobao.top.analysis.job.JobMergedResult;
import com.taobao.top.analysis.job.JobTaskStatus;
import com.taobao.top.analysis.node.IJobBuilder;
import com.taobao.top.analysis.node.IJobExporter;
import com.taobao.top.analysis.node.IJobManager;
import com.taobao.top.analysis.node.IJobResultMerger;
import com.taobao.top.analysis.node.event.JobRequestEvent;
import com.taobao.top.analysis.node.event.JobResponseEvent;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class JobManager implements IJobManager {

	private IJobBuilder jobBuilder;
	private IJobExporter jobExporter;
	private IJobResultMerger jobResultMerger;
	private MasterConfig config;
	
	private Map<String,Job> jobs;
	
	
	/**
	 * slave 返回得结果数据
	 */
	private java.util.concurrent.BlockingQueue<JobTaskResult> jobTaskResultsQueue;
	/**
	 * 任务池
	 */
	private ConcurrentMap<String, JobTask> jobTaskPool;
	/**
	 * 任务状态池
	 */
	private ConcurrentMap<String, JobTaskStatus> statusPool;
	/**
	 * 未何并的中间结果
	 */
	private BlockingQueue<JobMergedResult> resultQueue;
	

	@Override
	public void init() throws AnalysisException {
		//获得任务数量
		jobs = jobBuilder.build(config.getJobsSource());		
		
		if (jobs == null || (jobs != null && jobs.size() == 0))
			throw new AnalysisException("jobs should not be empty!");
		
		jobTaskPool = new ConcurrentHashMap<String, JobTask>();
		statusPool = new ConcurrentHashMap<String, JobTaskStatus>();
		resultQueue = new LinkedBlockingQueue<JobMergedResult>();
		jobTaskResultsQueue = new LinkedBlockingQueue<JobTaskResult>();
			
		addJobsToPool();
		
		jobBuilder.init();
		jobExporter.init();
		jobResultMerger.init();
	}

	
	@Override
	public void releaseResource() {
		
		jobs.clear();
		jobTaskPool.clear();
		statusPool.clear();
		resultQueue.clear();
		jobTaskResultsQueue.clear();
		
		jobBuilder.releaseResource();
		jobExporter.releaseResource();
		jobResultMerger.releaseResource();
	}
	
	@Override
	public void checkJobStatus() throws AnalysisException {
		
		//通过外部事件激发重新载入配置
		if (jobBuilder.isNeedRebuild())
		{
			jobs = jobBuilder.rebuild();
			
			addJobsToPool();
			
			if (jobs == null || (jobs != null && jobs.size() == 0))
				throw new AnalysisException("jobs should not be empty!");
		}
		
		checkTaskStatus();
		
		mergeAndExportJobs();
		
	}
	
	protected void addJobsToPool()
	{
		for(Job job : jobs.values())
		{
			List<JobTask> tasks = job.getJobTasks();
			
			for(JobTask task : tasks)
			{
				jobTaskPool.put(task.getTaskId(), task);
				statusPool.put(task.getTaskId(), task.getStatus());
			}	
		}
		
	}
	
	protected void mergeAndExportJobs()
	{
		for(Job job : jobs.values())
		{
			if (job.needMerge())
			{
				jobResultMerger.merge(job);		
			}
			
			if (job.needExport())
			{
				jobExporter.export(job);
			}
			
			if (job.needReset())
			{
				job.reset();
				
				List<JobTask> tasks = job.getJobTasks();
				
				for(JobTask task : tasks)
				{
					statusPool.put(task.getTaskId(), task.getStatus());
				}	
			}
		}
	}
	
	
	//重置在指定时间内未完成的任务
	protected void checkTaskStatus()
	{
		Iterator<String> taskIds = statusPool.keySet().iterator();
		
		while(taskIds.hasNext())
		{
			String taskId = taskIds.next();
			
			JobTaskStatus taskStatus = statusPool.get(taskId);
			JobTask jobTask = jobTaskPool.get(taskId);
			
			if (taskStatus == JobTaskStatus.DOING && 
					System.currentTimeMillis() - jobTask.getStartTime() >= jobTask.getTaskRecycleTime())
			{
				if (statusPool.replace(taskId, JobTaskStatus.DOING, JobTaskStatus.UNDO))
				{
					jobTask.setStatus(JobTaskStatus.UNDO);
				}
			}	
		}
	}
	
	@Override
	public MasterConfig getConfig() {
		return config;
	}


	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}

	@Override
	public Map<String,Job> getJobs() {
		return jobs;
	}

	@Override
	public void setJobs(Map<String,Job> jobs) {
		this.jobs = jobs;
	}
	
	@Override
	public IJobBuilder getJobBuilder() {
		return jobBuilder;
	}

	
	@Override
	public void setJobBuilder(IJobBuilder jobBuilder) {
		this.jobBuilder = jobBuilder;
	}

	
	@Override
	public IJobExporter getJobExporter() {
		return jobExporter;
	}

	
	@Override
	public void setJobExporter(IJobExporter jobExporter) {
		this.jobExporter = jobExporter;
	}

	
	@Override
	public IJobResultMerger getJobResultMerger() {
		return jobResultMerger;
	}

	
	@Override
	public void setJobResultMerger(IJobResultMerger jobResultMerger) {
		this.jobResultMerger = jobResultMerger;
	}


	@Override
	public void getUnDoJobTasks(JobRequestEvent requestEvent) {
		
	}


	@Override
	public void addTaskResultToQueue(JobResponseEvent jobResponseEvent) {
		
	}


	@Override
	public void exportJobData(String jobName) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void loadJobData(String jobName) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void clearJobData(String jobName) {
		// TODO Auto-generated method stub
		
	}

}
