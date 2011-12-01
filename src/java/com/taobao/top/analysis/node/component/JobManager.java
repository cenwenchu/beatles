/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobBuilder;
import com.taobao.top.analysis.node.IJobExporter;
import com.taobao.top.analysis.node.IJobManager;
import com.taobao.top.analysis.node.IJobResultMerger;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobMergedResult;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.node.job.JobTaskStatus;
import com.taobao.top.analysis.util.NamedThreadFactory;

/**
 * JobManager会被MasterNode以单线程方式调用
 * 需要注意的是所有的内置Builder,Exporter,ResultMerger,ServerConnector都自己必须保证处理速度
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class JobManager implements IJobManager {
	
	private final Log logger = LogFactory.getLog(JobManager.class);

	private IJobBuilder jobBuilder;
	private IJobExporter jobExporter;
	private IJobResultMerger jobResultMerger;
	private MasterConfig config;
	private MasterNode masterNode;
	private Map<String,Job> jobs;
	
	
	/**
	 * slave 返回得结果数据
	 */
	private Map<String,BlockingQueue<JobTaskResult>> jobTaskResultsQueuePool;
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
	private Map<String,BlockingQueue<JobMergedResult>> branchResultQueuePool;
	
	private ThreadPoolExecutor eventProcessThreadPool;
	

	@Override
	public void init() throws AnalysisException {
		//获得任务数量
		jobBuilder.setConfig(config);
		jobExporter.setConfig(config);
		jobResultMerger.setConfig(config);
		
		jobs = jobBuilder.build();		
		
		if (jobs == null || (jobs != null && jobs.size() == 0))
			throw new AnalysisException("jobs should not be empty!");
		
		jobTaskPool = new ConcurrentHashMap<String, JobTask>();
		statusPool = new ConcurrentHashMap<String, JobTaskStatus>();
		jobTaskResultsQueuePool = new HashMap<String,BlockingQueue<JobTaskResult>>();
		branchResultQueuePool = new HashMap<String,BlockingQueue<JobMergedResult>>();
		
		for(String jobName : jobs.keySet())
		{
			jobTaskResultsQueuePool.put(jobName, new LinkedBlockingQueue<JobTaskResult>());
			branchResultQueuePool.put(jobName, new LinkedBlockingQueue<JobMergedResult>());
		}
		
		eventProcessThreadPool = new ThreadPoolExecutor(
				this.config.getMaxJobEventWorker(),
				this.config.getMaxJobEventWorker(), 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("jobManagerEventProcess_worker"));
			
		addJobsToPool();
		
		jobBuilder.init();
		jobExporter.init();
		jobResultMerger.init();
	}

	
	@Override
	public void releaseResource() {
		
		try
		{
			eventProcessThreadPool.shutdown();
		}
		finally
		{
		
			jobs.clear();
			jobTaskPool.clear();
			statusPool.clear();
			jobTaskResultsQueuePool.clear();
			branchResultQueuePool.clear();	
		
		
			jobBuilder.releaseResource();
			jobExporter.releaseResource();
			jobResultMerger.releaseResource();
		}
	}
	
	//分配任务和结果提交处理由于是单线程处理，
	//因此本身不用做状态池并发控制，将消耗较多的发送操作交给ServerConnector多线程操作
	@Override
	public void getUnDoJobTasks(GetTaskRequestEvent requestEvent) {
		
		String jobName = requestEvent.getJobName();
		int jobCount = requestEvent.getRequestJobCount();
		List<JobTask> jobTasks = new ArrayList<JobTask>();
		
		//指定job
		if (jobName != null && jobs.containsKey(jobName))
		{
			Job job = jobs.get(jobName);
			
			List<JobTask> tasks = job.getJobTasks();
			
			for(JobTask jobTask : tasks)
			{
				if (jobTask.getStatus().equals(JobTaskStatus.UNDO))
				{
					statusPool.put(jobTask.getTaskId(), JobTaskStatus.DOING);
					jobTask.setStatus(JobTaskStatus.DOING);
					jobTask.setStartTime(System.currentTimeMillis());
					jobTasks.add(jobTask);
					
					if (jobTasks.size() == jobCount)
						break;
				}
			}
		}
		else
		{
			Iterator<String> taskIds = statusPool.keySet().iterator();
			
			while(taskIds.hasNext())
			{
				String taskId = taskIds.next();
				JobTask jobTask = jobTaskPool.get(taskId); 
				
				if (statusPool.get(taskId).equals(JobTaskStatus.UNDO))
				{
					statusPool.put(taskId, JobTaskStatus.DOING);
					jobTask.setStatus(JobTaskStatus.DOING);
					jobTask.setStartTime(System.currentTimeMillis());
					jobTasks.add(jobTask);
					
					if (jobTasks.size() == jobCount)
						break;
				}				
			}
		}

		masterNode.echoGetJobTasks(requestEvent.getSequence(),jobTasks);
	}


	//分配任务和结果提交处理由于是单线程处理，
	//因此本身不用做状态池并发控制，将消耗较多的发送操作交给ServerConnector多线程操作
	@Override
	public void addTaskResultToQueue(SendResultsRequestEvent jobResponseEvent) {
		
		JobTaskResult jobTaskResult = jobResponseEvent.getJobTaskResult();
		
		if (jobTaskResult.getTaskIds() != null && jobTaskResult.getTaskIds().size() > 0)
		{
			//判断是否是过期的一些老任务数据，根据task和taskresult的createtime来判断
			if (jobTaskResult.getCreatTime() != jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getCreatTime())
			{
				logger.warn("old task result will be discard!");
				return;
			}
			
			//先放入队列，防止小概率多线程并发问题
			jobTaskResultsQueuePool.get(jobs.get(jobTaskResult.getTaskIds().get(0)).getJobName()).offer(jobTaskResult);
			
			for(int i = 0 ; i < jobTaskResult.getTaskIds().size(); i++)
			{
				String taskId = jobTaskResult.getTaskIds().get(i);
				JobTask jobTask = jobTaskPool.get(taskId);
				Job job = jobs.get(jobTask.getJobName());
				
				if (statusPool.replace(taskId, JobTaskStatus.DOING, JobTaskStatus.DONE)
						|| statusPool.replace(taskId, JobTaskStatus.UNDO, JobTaskStatus.DONE))
				{
					jobTask.setStatus(JobTaskStatus.DONE);
					jobTask.setEndTime(System.currentTimeMillis());
					job.getCompletedTaskCount().incrementAndGet();
				}
			}	
			
		}
		
		masterNode.echoSendJobTaskResults(jobResponseEvent.getSequence(),"success");
	}


	@Override
	public void exportJobData(String jobName) {
		
		if (jobs.containsKey(jobName))
		{
			jobExporter.exportEntryData(jobs.get(jobName));
		}
		else
		{
			logger.error("exportJobData do nothing, jobName " +  jobName + " not exist!");
		}
		
	}


	@Override
	public void loadJobData(String jobName) {
		if (jobs.containsKey(jobName))
		{
			jobExporter.loadEntryData(jobs.get(jobName));
		}
		else
		{
			logger.error("exportJobData do nothing, jobName " +  jobName + " not exist!");
		}
	}
	
	@Override
	public void loadJobDataToTmp(String jobName) {
		if (jobs.containsKey(jobName))
		{
			jobExporter.loadEntryDataToTmp(jobs.get(jobName));
		}
		else
		{
			logger.error("exportJobData do nothing, jobName " +  jobName + " not exist!");
		}
	}


	@Override
	public void clearJobData(String jobName) {
		Job job = jobs.get(jobName);
		
		if (job != null)
		{
			job.getJobResult().clear();
		}
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
			//一个job只有一个线程会去处理，多个job不干扰
			if (!job.isMerging() && job.needMerge())
			{
				final Job j = job;
				final BlockingQueue<JobMergedResult> branchResultQueue = branchResultQueuePool.get(j.getJobName());
				final BlockingQueue<JobTaskResult> jobTaskResultsQueue = jobTaskResultsQueuePool.get(j.getJobName());
				
				eventProcessThreadPool.execute
						(new Runnable()
						{
							public void run()
							{
								j.setMerging(true);
								try
								{
									jobResultMerger.merge(j,branchResultQueue,jobTaskResultsQueue,true);
								}
								finally
								{
									j.setMerging(false);
								}
							}
						});			
			}
			
			//一个job只有一个线程会去处理,多个job不干扰
			if (!job.isExporting() && job.needExport())
			{
				final Job j = job;
				eventProcessThreadPool.execute
						(new Runnable()
						{
							public void run()
							{
								j.setExporting(true);
								try
								{
									jobExporter.exportReport(j,false);
									j.setExported(true);
								}
								finally
								{
									j.setExporting(false);
								}
							}
						});	
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
					jobTask.getRecycleCounter().incrementAndGet();
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
	public void setMasterNode(MasterNode masterNode) {
		this.masterNode = masterNode;
	}
	
}
