/**
 * 
 */
package com.taobao.top.analysis.node.component;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.event.SlaveNodeEvent;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.statistics.IStatisticsEngine;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.data.Rule;
import com.taobao.top.analysis.util.NamedThreadFactory;

/**
 * 分布式集群 Slave Node （可以是虚拟机内部的）
 * 使用方式参考MasterSlaveIntegrationTest 类
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class SlaveNode extends AbstractNode<SlaveNodeEvent,SlaveConfig>{

	private static final Log logger = LogFactory.getLog(SlaveNode.class);
	
	/**
	 * 与master通信的组件
	 */
	ISlaveConnector slaveConnector;
	/**
	 * 分析引擎
	 */
	IStatisticsEngine statisticsEngine;
	/**
	 * 结果合并组件，用于一次获取多个任务，合并任务结果返回给master的情况，分担master合并压力
	 */
	IJobResultMerger jobResultMerger;
	/**
	 * 会话序列生成器
	 */
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
		event.setMaxEventHoldTime(config.getMaxClientEventWaitTime());
		
		if (config.getJobName() != null)
			event.setJobName(config.getJobName());
		
		JobTask[] jobTasks = slaveConnector.getJobTasks(event);	
		
		
		if (jobTasks != null && jobTasks.length > 0)
		{
			//只有一个任务的情况
			if (jobTasks.length == 1)
			{
				try 
				{
					//计算并输出
					JobTaskResult jobTaskResult = statisticsEngine.doAnalysis(jobTasks[0]);
					if (jobTaskResult != null)
					{
						handleTaskResult(jobTasks[0],jobTaskResult);
					}
				} 
				catch (Exception e) {
					logger.error("SlaveNode send result error.",e);
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
	
	void handleTaskResult(JobTask jobTask,JobTaskResult jobTaskResult)
	{
		statisticsEngine.doExport(jobTask,jobTaskResult);
		
		Rule rule = jobTask.getStatisticsRule();
		Map<String, String> report2Master = rule.getReport2Master();
		
		//判断是否需要分开多个master投递结果
		if (report2Master!= null 
				&& report2Master.size() > 0)
		{
			Map<String, Map<String, Object>> _entryResults = jobTaskResult.getResults();
			
			//第一级String为masteraddress
			Map<String,Map<String, Map<String, Object>>> _masterEntryResults = new HashMap<String,Map<String, Map<String, Object>>>();
			
			for(String entryId : _entryResults.keySet())
			{
				ReportEntry reportEntry = rule.getEntryPool().get(entryId);
				List<String> reports = reportEntry.getReports();
				
				for(String r : reports)
				{
					String master = report2Master.get(r);
					
					if (master == null)
					{
						master = config.getMasterAddress()+":"+config.getMasterPort();
						
						logger.error("report" + r + " has no master process,send to default master.");
					}
					else
						master = master.substring(master.indexOf(":")+1);
					
					
					if (_masterEntryResults.get(master) == null)
					{
						_masterEntryResults.put(master, new HashMap<String,Map<String,Object>>());
					}
					
					_masterEntryResults.get(master).put(entryId, _entryResults.get(entryId));		
				}
				
			}
			
			SendResultsRequestEvent event = generateSendResultsRequestEvent(jobTaskResult);
			
			for(Entry<String,Map<String,Map<String,Object>>> e : _masterEntryResults.entrySet())
			{
				event.getJobTaskResult().setResults(e.getValue());
				slaveConnector.sendJobTaskResults(event,e.getKey());
				
				logger.info("send piece result to master :" + e.getKey());
			}
			
		}
		else
			slaveConnector.sendJobTaskResults(generateSendResultsRequestEvent(jobTaskResult),
						config.getMasterAddress()+":"+config.getMasterPort());
	}
	
	public SendResultsRequestEvent generateSendResultsRequestEvent(JobTaskResult jobTaskResult)
	{
		SendResultsRequestEvent responseEvent = new SendResultsRequestEvent(new StringBuilder()
				.append(System.currentTimeMillis()).append("-").append(sequenceGen.incrementAndGet()).toString());

		responseEvent.setJobTaskResult(jobTaskResult);
		responseEvent.setMaxEventHoldTime(slaveConnector.getConfig().getMaxClientEventWaitTime());

		return responseEvent;
	}

	@Override
	public void processEvent(SlaveNodeEvent event) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * 同一个Job的多个Task并行执行，并最后合并的模式处理
	 * @author fangweng
	 * email: fangweng@taobao.com
	 * 下午2:00:58
	 *
	 */
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
				//只有一个任务
				if (jobTasks.size() == 1)
				{
					try 
					{
						//计算并输出
						JobTaskResult jobTaskResult = statisticsEngine.doAnalysis(jobTasks.get(0));
						
						if (jobTaskResult != null)
						{
							handleTaskResult(jobTasks.get(0),jobTaskResult);
						}
	
					} 
					catch (Exception e) {
						logger.error("SlaveNode send result error.",e);
					}
				}
				else
				{
					final CountDownLatch taskCountDownLatch = new CountDownLatch(jobTasks.size());
					
					final List<JobTaskResult> taskResults = new ArrayList<JobTaskResult>();
					
					//同一个Job的多个Task并行执行
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
											logger.error(e,e);
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
					
					//合并分析结果
					JobTaskResult jobTaskResult = jobResultMerger.merge(jobTasks.get(0), taskResults,true);
					//输出
					if (jobTaskResult != null)
						handleTaskResult(jobTasks.get(0),jobTaskResult);
					
				}
			}
			catch(Exception ex)
			{
				logger.error("BundleTasksExecutable error.",ex);
			}
			finally
			{
				countDownLatch.countDown();
			}
			
		}
		
	}

}
