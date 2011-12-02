/**
 * 
 */
package com.taobao.top.analysis.node.component;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobResultMerger;
import com.taobao.top.analysis.node.connect.ISlaveConnector;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.SlaveNodeEvent;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.statistics.IStatisticsEngine;
import com.taobao.top.analysis.util.NamedThreadFactory;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class SlaveNode extends AbstractNode<SlaveNodeEvent,SlaveConfig>{

	private static final Log logger = LogFactory.getLog(SlaveNode.class);
	
	ISlaveConnector slaveConnector;
	IStatisticsEngine statisticsEngine;
	IJobResultMerger jobResultMerger;
	AtomicLong sequenceGen;
	
	/**
	 * 分析工作线程池
	 */
	private ThreadPoolExecutor analysisWorkerThreadPool;

	public IStatisticsEngine getStatisticsEngine() {
		return statisticsEngine;
	}

	public void setStatisticsEngine(IStatisticsEngine statisticsEngine) {
		this.statisticsEngine = statisticsEngine;
	}

	public ISlaveConnector getSlaveConnector() {
		return slaveConnector;
	}

	public void setSlaveConnector(ISlaveConnector slaveConnector) {
		this.slaveConnector = slaveConnector;
	}

	public IJobResultMerger getJobResultMerger() {
		return jobResultMerger;
	}

	public void setJobResultMerger(IJobResultMerger jobResultMerger) {
		this.jobResultMerger = jobResultMerger;
	}

	@Override
	public void init() throws AnalysisException {
		sequenceGen = new AtomicLong(0);
		slaveConnector.setConfig(config);
		statisticsEngine.setConfig(config);
		
		analysisWorkerThreadPool = new ThreadPoolExecutor(
				this.config.getAnalysisWorkerNum(),
				this.config.getAnalysisWorkerNum(), 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("analysisProcess_worker"));
		
		slaveConnector.init();
		statisticsEngine.init();
		jobResultMerger.init();
		
		if (logger.isInfoEnabled())
			logger.info("Slave init complete.");
	}

	@Override
	public void releaseResource() {
		
		sequenceGen.set(0);
		
		try
		{
			analysisWorkerThreadPool.shutdown();
		}
		finally
		{
			slaveConnector.releaseResource();
			statisticsEngine.releaseResource();
			jobResultMerger.releaseResource();
		}
		
		if (logger.isInfoEnabled())
			logger.info("Slave releaseResource complete.");
		
	}

	@Override
	public void process() {
		
		//尝试获取任务
		GetTaskRequestEvent event = new GetTaskRequestEvent(new StringBuilder()
			.append(System.currentTimeMillis()).append("-").append(sequenceGen.incrementAndGet()).toString());
		event.setRequestJobCount(config.getMaxTransJobCount());
		
		if (config.getJobName() != null)
			event.setJobName(config.getJobName());
		
		JobTask[] jobTasks = slaveConnector.getJobTasks(event);	
		
		if (jobTasks != null && jobTasks.length > 0)
		{
			if (jobTasks.length == 1)
			{
				try 
				{
					statisticsEngine.doExport(jobTasks[0],statisticsEngine.doAnalysis(jobTasks[0]));
				} 
				catch (Exception e) {
					logger.error(e);
				}
			}
			else
			{
				//同一个job的任务可以合并后在发送
				Map<String,List<JobTask>> taskBundle = new HashMap<String,List<JobTask>>();
				
				for(JobTask task : jobTasks)
				{
					String jobName = task.getJobName();
					
					List<JobTask> jobtasks = taskBundle.get(jobName);
					
					if (jobtasks == null)
					{
						jobtasks = new ArrayList<JobTask>();
						taskBundle.put(jobName, jobtasks);
					}
					
					jobtasks.add(task);
				}
				
				//起多个线程执行
				CountDownLatch countDownLatch = new CountDownLatch(taskBundle.size());
							
				for(List<JobTask> tasks : taskBundle.values())
				{
					analysisWorkerThreadPool.execute(new BundleTasksExecutable(tasks,countDownLatch));
				}
				
				try 
				{
					if (!countDownLatch.await(config.getMaxBundleProcessTime(), TimeUnit.SECONDS))
						logger.error("Bundle task execute timeout !");
					else
						if (logger.isInfoEnabled() && jobTasks != null && jobTasks.length > 0)
							logger.info("Bundle task execute complete! task count :" + jobTasks.length);
				} 
				catch (InterruptedException e) 
				{
					//do nothing
				}
				
			}
			
		}
		else
		{
			try 
			{
				Thread.sleep(config.getGetJobInterval());
			} 
			catch (InterruptedException e) 
			{
				//do nothing
			}
		}
		
	}

	@Override
	public void processEvent(SlaveNodeEvent event) {
		// TODO Auto-generated method stub
		
	}
	
	class BundleTasksExecutable implements java.lang.Runnable
	{
		List<JobTask> jobTasks;
		CountDownLatch countDownLatch;
		
		public BundleTasksExecutable(List<JobTask> jobTasks,CountDownLatch countDownLatch)
		{
			this.jobTasks = jobTasks;
			this.countDownLatch = countDownLatch;
		}
		
		@Override
		public void run() {
			try
			{
				if (jobTasks.size() == 1)
				{
					try 
					{
						statisticsEngine.doExport(jobTasks.get(0),statisticsEngine.doAnalysis(jobTasks.get(0)));
					} 
					catch (Exception e) {
						logger.error(e);
					}
				}
				else
				{
					final CountDownLatch taskCountDownLatch = new CountDownLatch(jobTasks.size());
					
					final List<JobTaskResult> taskResults = new ArrayList<JobTaskResult>();
					
					for(JobTask jobtask : jobTasks)
					{
						final JobTask j = jobtask;
						
						analysisWorkerThreadPool.execute(
								new Runnable()
								{
									public void run()
									{
										try 
										{
											taskResults.add(statisticsEngine.doAnalysis(j));
										} 
										catch (Exception e) 
										{
											logger.error(e);
										} 
										finally
										{
											taskCountDownLatch.countDown();
										}
									}
								}
								);
					}				
					
					
					try 
					{
						if (!taskCountDownLatch.await(config.getMaxTaskProcessTime(),TimeUnit.SECONDS))
							logger.error("task execute timeout !");
					} 
					catch (InterruptedException e) {
						//do nothing
					}
					
					JobTaskResult jobTaskResult = jobResultMerger.merge(jobTasks.get(0), taskResults,true);
						
					statisticsEngine.doExport(jobTasks.get(0), jobTaskResult);
				}
			}
			finally
			{
				countDownLatch.countDown();
			}
			
		}
		
	}

}
