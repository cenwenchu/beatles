/**
 * 
 */
package com.taobao.top.analysis.node.component;



import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobResultMerger;
import com.taobao.top.analysis.node.connect.ISlaveConnector;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.event.SlaveNodeEvent;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.node.operation.JobDataOperation;
import com.taobao.top.analysis.statistics.IStatisticsEngine;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.data.Rule;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.AnalyzerUtil;
import com.taobao.top.analysis.util.AnalyzerZKWatcher;
import com.taobao.top.analysis.util.NamedThreadFactory;
import com.taobao.top.analysis.util.Threshold;
import com.taobao.top.analysis.util.ZKUtil;

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
	 * Slave监控组件
	 */
	private SlaveMonitor monitor;
	
	/**
	 * 会话序列生成器
	 */
	AtomicLong sequenceGen;
	
	/**
	 * slave 所有计算消耗的时间,不包含命令获取的时间
	 */
	AtomicLong hardWorkTimer;
	
	/**
	 * 关闭状态,slave处于该状态时，不再向master请求新的任务;
	 * 并等待所有结果集发送完成
	 */
	private volatile boolean stopped = false;
	
	//防止大量写日志的阀
    transient Threshold threshold;
	
	/**
	 * 分析工作线程池
	 */
	private ThreadPoolExecutor analysisWorkerThreadPool;
	
	/**
	 * 任务池
	 */
	private Map<String, Long> doingTasks;
	
	/**
	 * 上次任务的执行时间
	 */
	private long lastTaskDoingTime;
	
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
	
	public void setMonitor(SlaveMonitor monitor) {
		this.monitor = monitor;
	}

	@Override
	public void init() throws AnalysisException {
		sequenceGen = new AtomicLong(0);
		hardWorkTimer = new AtomicLong(0);
		slaveConnector.setConfig(config);
		statisticsEngine.setConfig(config);
		monitor.setConfig(config);
		threshold = new Threshold(5000);
		monitor.setSlaveConnector(slaveConnector);
		
		analysisWorkerThreadPool = new ThreadPoolExecutor(
				this.config.getAnalysisWorkerNum(),
				this.config.getAnalysisWorkerNum(), 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("analysisProcess_worker"));
		
		slaveConnector.setSlaveNode(this);
		slaveConnector.init();
		statisticsEngine.init();
		jobResultMerger.init();
		monitor.init();
		doingTasks = new ConcurrentHashMap<String, Long>();
		
		//增加一块对于zookeeper的支持
		if (StringUtils.isNotEmpty(config.getZkServer()))
		{
			try
			{
				AnalyzerZKWatcher<SlaveConfig> analyzerZKWatcher = new AnalyzerZKWatcher<SlaveConfig>(config);
				zk = new ZooKeeper(config.getZkServer(),3000,analyzerZKWatcher);
				analyzerZKWatcher.setZk(zk);
				
				//每次启动时都先检查是否有根目录
				ZKUtil.createGroupNodesIfNotExist(zk,config.getGroupId());
				
				ZKUtil.updateOrCreateNode(zk,ZKUtil.getGroupSlaveZKPath(config.getGroupId())
						+ "/" + config.getSlaveName(),config.marshal().getBytes("UTF-8"));
				
			}
			catch(Exception ex)
			{
				logger.error("config to zk error!",ex);
			}
			
		}
		
		if (logger.isInfoEnabled())
			logger.info("Slave init complete.");
		lastTaskDoingTime = System.currentTimeMillis();
	}

	@Override
	public void releaseResource() {
	    this.stopped = true;
	    if(logger.isInfoEnabled())
	        logger.info("trying to release resource");
		
		sequenceGen.set(0);
		hardWorkTimer.set(0);
		
		long start = System.currentTimeMillis();
		try
		{
		    while(!doingTasks.isEmpty()) {
		        try {
		        Thread.sleep(5000);
		        } catch (Throwable e) {
		        }
		        if(System.currentTimeMillis() - start > 50000)
		            break;
		    }
			if (analysisWorkerThreadPool != null)
				analysisWorkerThreadPool.shutdown();
			if(logger.isInfoEnabled())
	            logger.info("analysisWorkerThreadPool shutdown");
			
		}
		finally
		{
			if (slaveConnector != null)
				slaveConnector.releaseResource();
			if(logger.isInfoEnabled())
                logger.info("slaveConnector releaseResource");
			
			if (statisticsEngine != null)
				statisticsEngine.releaseResource();
			if(logger.isInfoEnabled())
                logger.info("statisticsEngine releaseResource");
			
			if (jobResultMerger != null)
				jobResultMerger.releaseResource();
			if(logger.isInfoEnabled())
                logger.info("jobResultMerger releaseResource");
			
			if(monitor != null) {
				monitor.releaseResource();
			}
			if(logger.isInfoEnabled()) {
                logger.info("monitor releaseResource");
			}
		}
		
		//增加一块对于zookeeper的支持
		if (StringUtils.isNotEmpty(config.getZkServer()) && zk != null)
		{
			try
			{
				ZKUtil.deleteNode(zk,ZKUtil.getGroupSlaveZKPath(config.getGroupId())
						+ "/" + config.getSlaveName());
				
			}
			catch(Exception ex)
			{
				logger.error("delete zk node error!",ex);
			}
			
		}
		
		if (logger.isInfoEnabled())
			logger.info("Slave releaseResource complete.");
		
	}

	@Override
	public void process() {
	    //状态处于关闭状态时，不再向master请求新的任务
	    if(stopped)
	        return;
		
		//尝试获取任务
		GetTaskRequestEvent event = new GetTaskRequestEvent(new StringBuilder()
			.append(System.currentTimeMillis()).append("-").append(sequenceGen.incrementAndGet()).toString());
		event.setRequestJobCount(config.getMaxTransJobCount());
		event.setMaxEventHoldTime(config.getMaxClientEventWaitTime());
		
		if (config.getJobName() != null)
			event.setJobName(config.getJobName());
		
		// 拉任务开始时间
		long start = System.currentTimeMillis();
		JobTask[] jobTasks = slaveConnector.getJobTasks(event);
		// 统计平均拉取任情况
		monitor.putPullTaskConsumeTime(System.currentTimeMillis()- start, jobTasks == null ? 0 : jobTasks.length);
		
		if (System.currentTimeMillis() - lastTaskDoingTime > this.config.getMaxIdleTime() * 1000) {
            AnalyzerUtil.sendOutAlert(java.util.Calendar.getInstance(), this.config.getAlertUrl(),
                this.config.getAlertFrom(), this.config.getAlertModel(), this.config.getAlertWangWang(),
                "there's no jobs to do. Please have a check at the master. Time:" + ((System.currentTimeMillis() - lastTaskDoingTime)/1000) + "s");
        }
		
		if (jobTasks != null && jobTasks.length > 0)
		{
		    lastTaskDoingTime = System.currentTimeMillis();
		    
			// 增加成功拉取的任务数
			monitor.incPulledTaskCount(jobTasks.length);
			for(JobTask jobTask : jobTasks) {
			    doingTasks.put(jobTask.getTaskId() + "_" + jobTask.getJobEpoch(), System.currentTimeMillis());
			    logger.info("doing tasks : " + doingTasks.size());
			}
			
		    StringBuilder sb = new StringBuilder("receive tasks:{");
		    for(JobTask task : jobTasks) {
		        sb.append(task.getTaskId()).append(",").append(task.getInput()).append(";");
		    }
		    sb.append("}");
		    if(logger.isWarnEnabled())
		        logger.warn(sb.toString());
			//比较初略的统计一下分析时间
			long timer = System.currentTimeMillis();
			
			//只有一个任务的情况
			if (jobTasks.length == 1)
			{
				try 
				{
					//计算并输出
					JobTaskResult jobTaskResult = statisticsEngine.doAnalysis(jobTasks[0]);
					
					long taskConsumeTime = System.currentTimeMillis() - timer;
					jobTaskResult.setEfficiency((taskConsumeTime + this.hardWorkTimer.get())
									/(System.currentTimeMillis()  - this.nodeStartTimeStamp));
					
					if (jobTaskResult != null) {
						handleTaskResult(jobTasks[0],jobTaskResult);
					}
					doingTasks.remove(jobTasks[0].getTaskId() + "_" + jobTasks[0].getJobEpoch());
				} 
				catch (Exception e) {
					logger.error("SlaveNode send result error. Tasks:[" + jobTasks[0].getJobName() + "," + jobTasks[0].getInput() + "]",e);
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
					analysisWorkerThreadPool.execute(new BundleTasksExecutable(tasks,countDownLatch,timer));
				}
				
				try 
				{
					if (!countDownLatch.await(config.getMaxBundleProcessTime(), TimeUnit.SECONDS)) {
						// 如果执行超时, 没有进一步处理啊
					    sb = new StringBuilder("Bundle task execute timeout !tasks:{");
                        for(JobTask task : jobTasks) {
                            sb.append(task.getTaskId()).append(",").append(task.getInput()).append(";");
                        }
                        sb.append("}");
						logger.error(sb.toString());
					} else {
						if (logger.isInfoEnabled() && jobTasks != null && jobTasks.length > 0) {
							logger.info("Bundle task execute complete! task count :" + jobTasks.length);
						}
					}
				} 
				catch (InterruptedException e) 
				{
					//do nothing
				}
				
			}
			
			this.hardWorkTimer.addAndGet(System.currentTimeMillis() - timer);
			
		}
		else
		{
			try 
			{
				Thread.sleep(config.getJobInterval());
			} 
			catch (InterruptedException e) 
			{
				//do nothing
			}
		}
		
	}
	
	void handleTaskResult(final JobTask jobTask,JobTaskResult jobTaskResult)
	{
		// 统计任务执行情况
		monitor.executedTask(getMaxTime(jobTaskResult), jobTaskResult.getTaskExecuteInfos().values());
		
	    if(logger.isInfoEnabled()) {
	        logger.info("start to handle task result");
	    }
		statisticsEngine.doExport(jobTask,jobTaskResult);
		
		final Rule rule = jobTask.getStatisticsRule();
		Map<String, String> report2Master = rule.getReport2Master();
		if(logger.isInfoEnabled()) {
		    for(String key : report2Master.keySet()) {
		        logger.info("report to master : " + report2Master.get(key));
		    }
		}
		
		//判断是否需要分开多个master投递结果
		if (report2Master!= null 
				&& report2Master.size() > 0)
		{
		    if(logger.isInfoEnabled()) {
	            logger.info("start to send result to multi master");
	        }
			Map<String, Map<String, Object>> _entryResults = jobTaskResult.getResults();
			
			//第一级String为masteraddress
			Map<String,Map<String, Map<String, Object>>> _masterEntryResults = new HashMap<String,Map<String, Map<String, Object>>>();
			
			Set<String> masters = jobTask.getStatisticsRule().getMasters();
			
			for(String entryId : _entryResults.keySet())
			{
				ReportEntry reportEntry = rule.getEntryPool().get(entryId);
				Set<String> reports = reportEntry.getReports();
				if(reports == null || reports.size() == 0) {
				    String master = config.getMasterAddress()+":"+config.getMasterPort();
				    logger.error("reportEntry has no report " + reportEntry.getId());
				    if (_masterEntryResults.get(master) == null)
                    {
                        _masterEntryResults.put(master, new HashMap<String,Map<String,Object>>());
                    }
                    
                    _masterEntryResults.get(master).put(entryId, _entryResults.get(entryId));
				}
				
				for(String r : reports)
				{
					String master = report2Master.get(r);
					
					if (master == null)
					{
						master = config.getMasterAddress()+":"+config.getMasterPort();
						
						if(!threshold.sholdBlock())
						    logger.error("report" + r + " has no master process,send to default master.");
					}
					
					if (_masterEntryResults.get(master) == null)
					{
						_masterEntryResults.put(master, new HashMap<String,Map<String,Object>>());
					}
					
					_masterEntryResults.get(master).put(entryId, _entryResults.get(entryId));		
				}
				
			}
			
			StringBuilder sb = new StringBuilder(_masterEntryResults.size());
			sb.append("( keys :");
			for(String s : _masterEntryResults.keySet()) {
			    sb.append(s).append(",");
			}
			sb.append(" )");
			logger.info(sb.toString());
			
			//批量发送消息
			final CountDownLatch taskCountDownLatch = new CountDownLatch(_masterEntryResults.size());
					
            for (Entry<String, Map<String, Map<String, Object>>> e : _masterEntryResults.entrySet()) {
                final Entry<String, Map<String, Map<String, Object>>> entrySet = e;
                final JobTaskResult tResult = jobTaskResult.cloneWithOutResults();
                tResult.setResults(entrySet.getValue());
                tResult.setJobName(jobTask.getJobName());
                tResult.setResultKey(entrySet.getKey());

                if (!this.config.getMultiSendResult()) {
                    try {
                        // 这里应该是这样的吧：
                        String masterAddr = entrySet.getKey();
                        // String masterAddr =
                        // entrySet.getKey().substring(entrySet.getKey().indexOf(":")+1);
                        SendResultsRequestEvent event = generateSendResultsRequestEvent(tResult);
                        String result = slaveConnector.sendJobTaskResults(event, entrySet.getKey());
                        if (result == null) {
                            Thread.sleep(100);
                            logger.warn("try to send result to master : " + masterAddr + " again.");
                            result = slaveConnector.sendJobTaskResults(event, masterAddr);

                            // 开始写入本地文件
                            if (result == null) {
                                logger.error(new StringBuilder("send result to master : ").append(entrySet.getKey())
                                    .append(" fail again! now to write file to local,jobName : ")
                                    .append(jobTask.getJobName()).toString());

                                String destFile = getTempStoreDataFile(entrySet.getKey(), jobTask.getJobName());

                                if (destFile != null) {
                                    JobDataOperation.export(event.getJobTaskResult().getResults(), destFile, false,
                                        false, (new HashMap<String, Long>()));
                                }
                            }
                        }

                        logger.info("send piece result to master :" + entrySet.getKey() + ", size:"
                                + entrySet.getValue().size() + ", result:" + result);
                    }
                    catch (Throwable e1) {
                        logger.error(e1, e1);
                    }
                }
                else {

                    analysisWorkerThreadPool.execute(new Runnable() {
                        public void run() {
                            try {
                                // 这里应该是这样的吧：
                                String masterAddr = entrySet.getKey();
                                // String masterAddr =
                                // entrySet.getKey().substring(entrySet.getKey().indexOf(":")+1);
                                SendResultsRequestEvent event = generateSendResultsRequestEvent(tResult);
                                String result = slaveConnector.sendJobTaskResults(event, entrySet.getKey());

                                // 做一次重试
                                if (result == null) {
                                    Thread.sleep(100);
                                    logger.warn("try to send result to master : " + masterAddr + " again.");
                                    result = slaveConnector.sendJobTaskResults(event, masterAddr);

                                    // 开始写入本地文件
                                    if (result == null) {
                                        logger.error(new StringBuilder("send result to master : ")
                                            .append(entrySet.getKey())
                                            .append(" fail again! now to write file to local,jobName : ")
                                            .append(jobTask.getJobName()).toString());

                                        String destFile = getTempStoreDataFile(entrySet.getKey(), jobTask.getJobName());

                                        if (destFile != null) {
                                            JobDataOperation.export(event.getJobTaskResult().getResults(), destFile,
                                                false, false, (new HashMap<String, Long>()));
                                        }
                                    }
                                }

                                logger.info("send piece result to master :" + entrySet.getKey() + ", size:"
                                        + entrySet.getValue().size() + ", result:" + result);
                            }
                            catch (Exception e) {
                                logger.error(e, e);
                            }
                            finally {
                                taskCountDownLatch.countDown();
                            }
                        }
                    });
                }
            }
            if (config.getMultiSendResult()) {
                try {
                    if (!taskCountDownLatch.await(this.config.getMaxSendResultTime(), TimeUnit.SECONDS))
                        logger.error("send piece result to master timeout !");
                }
                catch (InterruptedException e2) {
                    // do nothing
                }
            }
			
			//暂时采用策略，将所有结果向所有其他master投递一个空的结果集，以确保所有master都有收到
            //任务结束的通知，这里代码很丑陋啊~~
			final CountDownLatch taskCountDown = new CountDownLatch((masters.size() - _masterEntryResults.size()));
			for(final String master : masters) {
                if(_masterEntryResults.get(master) != null)
                    continue;
                final JobTaskResult tResult = jobTaskResult.cloneWithOutResults();
                tResult.setJobName(jobTask.getJobName());
                analysisWorkerThreadPool.execute(
                    new Runnable()
                    {
                        public void run()
                        {
                            try 
                            {
                            	// 这里应该是这样的吧：
								String masterAddr = master;
                                // String masterAddr = master.substring(master.indexOf(":")+1);
                                SendResultsRequestEvent event = generateSendResultsRequestEvent(tResult);
                                String result = slaveConnector.sendJobTaskResults(event,masterAddr);
                                
                                //做一次重试
                                if (result == null)
                                {
                                    Thread.sleep(100);
                                    logger.warn("try to send result to master : " + masterAddr + " again.");
                                    result = slaveConnector.sendJobTaskResults(event,masterAddr);
                                    
                                    //开始写入本地文件
                                    if (result == null)
                                    {
                                        logger.error( new StringBuilder("send result to master : ")
                                            .append(master).append(" fail again! now to write file to local,jobName : ")
                                            .append(jobTask.getJobName()).toString());
                                        
                                        String destFile = getTempStoreDataFile(master,jobTask.getJobName());
                                        
                                        if (destFile != null)
                                        {
                                            JobDataOperation.export(event.getJobTaskResult().getResults(), destFile,false,false, (new HashMap<String, Long>()));
                                        }
                                    }
                                }
                                
                                logger.info("send empty result to master :" + master + ", result:" + result);
                            } 
                            catch (Exception e) 
                            {
                                logger.error(e,e);
                            } 
                            finally
                            {
                                taskCountDown.countDown();
                            }
                        }
                    }
                    );
            }
			
			try 
            {
                if (!taskCountDown.await(10,TimeUnit.SECONDS))
                    logger.error("send piece result to master timeout !");
            } 
            catch (InterruptedException e) {
                //do nothing
            }
			
		}
		else
			slaveConnector.sendJobTaskResults(generateSendResultsRequestEvent(jobTaskResult),
						config.getMasterAddress()+":"+config.getMasterPort());
		doingTasks.remove(jobTask.getTaskId() + "_" + jobTask.getJobEpoch());
	}
	
	String getTempStoreDataFile(String master,String jobName)
	{
		String destFileName = null;
		
		try
		{
			StringBuilder tempFile = new StringBuilder();
			tempFile.append(config.getSlaveName()).append(File.separator);
			tempFile.append(config.getTempStoreDataDir());
			
			File dest =  new File(tempFile.toString());
			
			if (!dest.exists() || (dest.exists() && !dest.isDirectory()))
			{
				new File(tempFile.toString()).mkdirs();
			}
			
			if (!config.getTempStoreDataDir().endsWith(File.separator))
				tempFile.append(File.separator);
			
			Calendar calendar = Calendar.getInstance();
			String currentTime = new StringBuilder()
					.append(calendar.get(Calendar.YEAR)).append("-")
					.append(calendar.get(Calendar.MONTH) + 1).append("-")
					.append(calendar.get(Calendar.DAY_OF_MONTH)).toString();
				
			tempFile.append(master).append(AnalysisConstants.SPLIT_KEY)
				.append(jobName).append(AnalysisConstants.SPLIT_KEY)
					.append(currentTime).append(AnalysisConstants.TEMP_MASTER_DATAFILE_SUFFIX);
			
			dest = new File(tempFile.toString());
			
			if (!dest.exists())
			{
				new File(tempFile.toString()).createNewFile();
			}
			
			destFileName = dest.getAbsolutePath();
		}
		catch(Exception ex)
		{
			logger.error("getTempStoreDataFile error.",ex);
		}
		
		return destFileName;
		
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
		switch(event.getEventCode())
		{
			case SUSPEND:
				this.suspendNode();
				break;
			
			case AWAKE:
				this.awaitNode();
				break;
			
			case CHANGE_MASTER:
				slaveConnector.changeMaster((String)event.getAttachment());
				break;
				
		}
		 
	}
	
	/**
	 * 取多个并发执行任务的最大消耗时间
	 * @param jobTaskResult
	 * @return
	 */
	private long getMaxTime(JobTaskResult jobTaskResult) {
		long max = 0;
		for(JobTaskExecuteInfo info : jobTaskResult.getTaskExecuteInfos().values()) {
			logger.warn(String.format("max=%d, info.getAnalysisConsume()=%d", max, info.getAnalysisConsume()));
			max = max < info.getAnalysisConsume() ? info.getAnalysisConsume() : max;
		}
		logger.warn(String.format("max=%d", max));
		return max;
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
		long timer;
		
		public BundleTasksExecutable(List<JobTask> jobTasks,CountDownLatch countDownLatch,long timer)
		{
			this.jobTasks = jobTasks;
			this.countDownLatch = countDownLatch;
			this.timer = timer;
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
					    long start = System.currentTimeMillis();
						//计算并输出
						JobTaskResult jobTaskResult = statisticsEngine.doAnalysis(jobTasks.get(0));
						
						jobTaskResult.setEfficiency((System.currentTimeMillis() - timer + hardWorkTimer.get())
								/(System.currentTimeMillis()  - nodeStartTimeStamp));
                        if (logger.isInfoEnabled())
                            logger.info("analysis task:" + jobTasks.get(0).getTaskId() + ","
                                    + jobTasks.get(0).getInput() + " uses:" + (System.currentTimeMillis() - start));

                        start = System.currentTimeMillis();
						if (jobTaskResult != null)
						{
							handleTaskResult(jobTasks.get(0),jobTaskResult);
						}
						if (logger.isInfoEnabled())
                            logger.info("send result task:" + jobTasks.get(0).getTaskId() + ","
                                    + jobTasks.get(0).getInput() + " uses:" + (System.currentTimeMillis() - start));
						doingTasks.remove(jobTasks.get(0).getTaskId() + "_" + jobTasks.get(0).getJobEpoch());
					} 
					catch (Exception e) {
						logger.error("SlaveNode send result error." + jobTasks.get(0).getJobName(),e);
					}
				}
				else
				{
					final CountDownLatch taskCountDownLatch = new CountDownLatch(jobTasks.size());
					
					final List<JobTaskResult> taskResults = new ArrayList<JobTaskResult>();
					
					//同一个Job的多个Task并行执行
					for(final JobTask jobtask : jobTasks)
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
											logger.error("analysis error : " + jobtask.getJobName(), e);
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
						if (!taskCountDownLatch.await(config.getMaxTaskProcessTime(),TimeUnit.SECONDS)) {
						    StringBuilder sb = new StringBuilder("task analysis timeout !tasks:{");
						    for(JobTask task : jobTasks) {
						        sb.append(task.getTaskId()).append(",").append(task.getInput()).append(";");
						    }
						    sb.append("}");
						    
							logger.error(sb.toString());
						}
					} 
					catch (InterruptedException e) {
						//do nothing
					}
					
					long start = System.currentTimeMillis();
					
					//合并分析结果
					JobTaskResult jobTaskResult = jobResultMerger.merge(jobTasks.get(0), taskResults,false,false);
					// 统计merge, 这里的时间并不科学, 应用线程并发, 时间可能被多计算了
					monitor.mergedTask(System.currentTimeMillis() - start, taskResults.size());
					
					jobTaskResult.setEfficiency((System.currentTimeMillis() - timer + hardWorkTimer.get())
							/(System.currentTimeMillis()  - nodeStartTimeStamp));
					if (logger.isInfoEnabled())
                        logger.info("merge result task:" + jobTasks.get(0).getTaskId() + ","
                                + jobTasks.get(0).getInput() + "," + taskResults.size() + ",uses:" + (System.currentTimeMillis() - start));
					
					start = System.currentTimeMillis();
					//输出
					if (jobTaskResult != null)
						handleTaskResult(jobTasks.get(0),jobTaskResult);
					for(JobTask jobtask : jobTasks) {
					    doingTasks.remove(jobtask.getTaskId() + "_" + jobtask.getJobEpoch());
					}
					if (logger.isInfoEnabled())
                        logger.info("send result task:" + jobTasks.get(0).getTaskId() + ","
                                + jobTasks.get(0).getInput() + " uses:" + (System.currentTimeMillis() - start));
					
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
