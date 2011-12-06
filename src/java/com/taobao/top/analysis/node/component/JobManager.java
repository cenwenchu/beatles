/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.util.ArrayList;
import java.util.Calendar;
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
import com.taobao.top.analysis.node.operation.JobDataOperation;
import com.taobao.top.analysis.util.AnalysisConstants;
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
	/**
	 * 所负责的管理的任务集合
	 */
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
	
	/**
	 * 事件处理线程
	 */
	private ThreadPoolExecutor eventProcessThreadPool;
	

	@Override
	public void init() throws AnalysisException {
		//获得任务数量
		jobBuilder.setConfig(config);
		jobExporter.setConfig(config);
		jobResultMerger.setConfig(config);
		
		jobBuilder.init();
		jobExporter.init();
		jobResultMerger.init();
		
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
		
		if(logger.isInfoEnabled())
			logger.info("jobManager init end, MaxJobEventWorker size : " + config.getMaxJobEventWorker());
		
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
			
			logger.info("jobManager releaseResource end");
			
		}
	}
	
	//分配任务和结果提交处理由于是单线程处理，
	//因此本身不用做状态池并发控制，将消耗较多的发送操作交给ServerConnector多线程操作
	@Override
	public void getUnDoJobTasks(GetTaskRequestEvent requestEvent) {
		
		String jobName = requestEvent.getJobName();
		int jobCount = requestEvent.getRequestJobCount();
		final List<JobTask> jobTasks = new ArrayList<JobTask>();
		
		if(logger.isInfoEnabled())
			if (jobName != null && jobs.containsKey(jobName))
				logger.info("receive event about get undoTask, jobName : " + jobName + "jobCount :" + jobCount);
			else
				logger.info("receive event about get undoTask, jobCount :" + jobCount);
		
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
					
					if (jobTasks.size() >= jobCount)
						break;
				}				
			}
		}

		final String sequence = requestEvent.getSequence();
		final Object channel = requestEvent.getChannel();
		
		//由于该操作比较慢，开线程执行，保证速度
		eventProcessThreadPool.execute(
				new Runnable()
						{
							public void run()
							{
								masterNode.echoGetJobTasks(sequence,jobTasks,channel);
							}
						});
			
		
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
			
			if (logger.isInfoEnabled())
			{
				StringBuilder ts = new StringBuilder("Receive slave analysis result, jobTaskIds : ");
				
				for(String id : jobTaskResult.getTaskIds())
					ts.append(id).append(" , ");
				
				logger.info(ts.toString());
			}
			
			//先放入队列，防止小概率多线程并发问题
			jobTaskResultsQueuePool.get(jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobName()).offer(jobTaskResult);
			
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
		
		final String sequence = jobResponseEvent.getSequence();
		final Object channel = jobResponseEvent.getChannel();
		
		eventProcessThreadPool.execute(
				new Runnable()
						{
							public void run()
							{
								masterNode.echoSendJobTaskResults(sequence,"success",channel);
							}
						});
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
			
			if (logger.isInfoEnabled())
				logger.info("clear job :" + job.getJobName() + " data.");
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
	
	//重新增加任务到任务池中
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
	
	//做合并和导出，重置任务的检查操作
	protected void mergeAndExportJobs()
	{
		for(Job job : jobs.values())
		{
			//需要合并该job的task
			if (!job.isMerging().get() && job.needMerge())
			{
				final Job j = job;
				final BlockingQueue<JobMergedResult> branchResultQueue = branchResultQueuePool.get(j.getJobName());
				final BlockingQueue<JobTaskResult> jobTaskResultsQueue = jobTaskResultsQueuePool.get(j.getJobName());
				
				if (j.isMerging().compareAndSet(false, true))
					eventProcessThreadPool.execute
						(new Runnable()
						{
							public void run()
							{
								try
								{
									jobResultMerger.merge(j,branchResultQueue,jobTaskResultsQueue,true);
								}
								finally
								{
									j.isMerging().set(false);
								}
							}
						});			
			}
			
			//需要导出该job的数据
			if (!job.isExporting().get() && job.needExport())
			{
				final Job j = job;
				
				if (j.isExporting().compareAndSet(false, true))
					eventProcessThreadPool.execute
						(new Runnable()
						{
							public void run()
							{
								try
								{
									//虽然是多线程，但还是阻塞模式来做
									jobExporter.exportReport(j,false);
									j.isExported().set(true);
								}
								finally
								{
									j.isExporting().set(false);
								}
								
								//判断是否需要开始导出中间结果,放在外部不妨碍下一次的处理
								exportOrCleanTrunk(j);
							}
						});	
			}
			
			//任务是否需要被重置
			if (job.needReset())
			{
				if (logger.isInfoEnabled())
					logger.info("job " + job.getJobName() + " be reset now.");
					
				job.reset();
				
				List<JobTask> tasks = job.getJobTasks();
				
				for(JobTask task : tasks)
				{
					statusPool.put(task.getTaskId(), task.getStatus());
				}	
			}
		}
	}
	
	/**
	 * 在导出数据以后，判断是否需要清空主干，是否需要导出主干
	 * @param job
	 */
	protected void exportOrCleanTrunk(Job job)
	{
		boolean needToSetJobResultNull = false;
		
		//判断是否到了报表的有效时间段，支持小时，日，月三种方式
		if (job.getJobConfig().getReportPeriodDefine().equals(AnalysisConstants.REPORT_PERIOD_DAY))
		{
			Calendar calendar = Calendar.getInstance();
			int now = calendar.get(Calendar.DAY_OF_MONTH);
			
			if (job.getReportPeriodFlag() != -1 && now != job.getReportPeriodFlag())
				needToSetJobResultNull = true;
			
			job.setReportPeriodFlag(now);
		}
		else
		{
			if (job.getJobConfig().getReportPeriodDefine().equals(AnalysisConstants.REPORT_PERIOD_HOUR))
			{
				Calendar calendar = Calendar.getInstance();
				int now = calendar.get(Calendar.HOUR_OF_DAY);
				
				if (job.getReportPeriodFlag() != -1 && now != job.getReportPeriodFlag())
					needToSetJobResultNull = true;
				
				job.setReportPeriodFlag(now);
			}
			else
			{
				if (job.getJobConfig().getReportPeriodDefine().equals(AnalysisConstants.REPORT_PERIOD_MONTH))
				{
					Calendar calendar = Calendar.getInstance();
					int now = calendar.get(Calendar.MONTH);
					
					if (job.getReportPeriodFlag() != -1 && now != job.getReportPeriodFlag())
						needToSetJobResultNull = true;
					
					job.setReportPeriodFlag(now);
				}
			}
		}
		
		if (needToSetJobResultNull)
		{
			job.setJobResult(null);
			
			//删除临时文件，防止重复载入使得清空不生效
			if (config.getSaveTmpResultToFile())
			{
				JobDataOperation jobDataOperation = new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_DEL_DATAFILE);
				jobDataOperation.run();
			}
			
			if (logger.isInfoEnabled())
				logger.info("job " + job.getJobName() + " report data be reset.it's a new start. ");
		}
		
		//清除主干数据，到时候自然会载入
		if (config.getSaveTmpResultToFile())
		{
			logger.warn("@ disk2Mem mode: " + job.getJobName() + " store trunk to disk now .");
			
			JobDataOperation jobDataOperation = new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_SETNULL_EXPORTDATA);
			jobDataOperation.run();
			
		}
		else
		{
			if (job.getLastExportTime() == 0 ||
					System.currentTimeMillis() - job.getLastExportTime() >= config.getExportInterval())
			{
				logger.warn("export job: " + job.getJobName() + " trunk to disk.");
				
				JobDataOperation jobDataOperation = new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_EXPORTDATA);
				jobDataOperation.run();
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
					System.currentTimeMillis() - jobTask.getStartTime() >= jobTask.getTaskRecycleTime() * 1000)
			{
				if (statusPool.replace(taskId, JobTaskStatus.DOING, JobTaskStatus.UNDO))
				{
					jobTask.setStatus(JobTaskStatus.UNDO);
					jobTask.getRecycleCounter().incrementAndGet();
					
					if (logger.isWarnEnabled())
						logger.warn("Task : " + jobTask.getTaskId() + " can't complete in time, it be recycle.");
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


	public Map<String, BlockingQueue<JobTaskResult>> getJobTaskResultsQueuePool() {
		return jobTaskResultsQueuePool;
	}


	public void setJobTaskResultsQueuePool(
			Map<String, BlockingQueue<JobTaskResult>> jobTaskResultsQueuePool) {
		this.jobTaskResultsQueuePool = jobTaskResultsQueuePool;
	}


	public Map<String, BlockingQueue<JobMergedResult>> getBranchResultQueuePool() {
		return branchResultQueuePool;
	}


	public void setBranchResultQueuePool(
			Map<String, BlockingQueue<JobMergedResult>> branchResultQueuePool) {
		this.branchResultQueuePool = branchResultQueuePool;
	}
	
}
