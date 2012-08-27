/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.channel.Channel;

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
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.node.job.JobTaskStatus;
import com.taobao.top.analysis.node.operation.JobDataOperation;
import com.taobao.top.analysis.node.operation.MergeJobOperation;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.AnalyzerZKWatcher;
import com.taobao.top.analysis.util.MasterDataRecoverWorker;
import com.taobao.top.analysis.util.NamedThreadFactory;
import com.taobao.top.analysis.util.ReportUtil;
import com.taobao.top.analysis.util.ZKUtil;


/**
 * JobManager会被MasterNode以单线程方式调用
 * 需要注意的是所有的内置Builder,Exporter,ResultMerger,ServerConnector都自己必须保证处理速度
 * 
 * @author fangweng
 * @Email fangweng@taobao.com 2011-11-28
 * 
 */
public class JobManager implements IJobManager {

    private static final Log logger = LogFactory.getLog(JobManager.class);

    private IJobBuilder jobBuilder;
    private IJobExporter jobExporter;
    private IJobResultMerger jobResultMerger;
    private MasterConfig config;
    private MasterNode masterNode;
    /**
     * 所负责的管理的任务集合
     */
    private Map<String, Job> jobs;

    /**
     * slave 返回得结果数据
     */
    private Map<String, BlockingQueue<JobTaskResult>> jobTaskResultsQueuePool;
    /**
     * 任务池
     * 任务池的分配方式可能会产生分配不均等，也不是很好的分配策略
     */
    private ConcurrentMap<String, JobTask> jobTaskPool;
    
    /**
     * 任务队列
     */
    private BlockingQueue<JobTask> undoTaskQueue;
    
    /**
     * 任务状态池
     */
    private ConcurrentMap<String, JobTaskStatus> statusPool;
    /**
     * 未何并的中间结果
     */
    private Map<String, BlockingQueue<JobMergedResult>> branchResultQueuePool;

    /**
     * 事件处理线程
     */
    private ThreadPoolExecutor eventProcessThreadPool;

    /**
     * 用于合并后台历史数据，当master出错时，slave会纪录一些数据在本地用于恢复
     */
    private MasterDataRecoverWorker masterDataRecoverWorker;
    
    
    /**
     * 关闭标志，重启关闭时置为true
     * 置为true后，不再分配新的任务，并等待任务merge完成
     * 导出中间结果
     */
    private volatile boolean stopped = false;
    
    ZooKeeper zk = null;

    @Override
    public void init() throws AnalysisException {
        // 获得任务数量
        jobBuilder.setConfig(config);
        jobExporter.setConfig(config);
        jobResultMerger.setConfig(config);

        jobBuilder.init();
        jobExporter.init();
        jobResultMerger.init();

        jobs = jobBuilder.build();
        for(Job job : jobs.values()) {
            job.reset(null);
        }

        if (jobs == null || (jobs != null && jobs.size() == 0))
            throw new AnalysisException("jobs should not be empty!");

        jobTaskPool = new ConcurrentHashMap<String, JobTask>();
        undoTaskQueue = new LinkedBlockingDeque<JobTask>();
        statusPool = new ConcurrentHashMap<String, JobTaskStatus>();
        jobTaskResultsQueuePool = new HashMap<String, BlockingQueue<JobTaskResult>>();
        branchResultQueuePool = new HashMap<String, BlockingQueue<JobMergedResult>>();

        for (String jobName : jobs.keySet()) {
            jobTaskResultsQueuePool.put(jobName, new LinkedBlockingQueue<JobTaskResult>());
            branchResultQueuePool.put(jobName, new LinkedBlockingQueue<JobMergedResult>());
        }

        eventProcessThreadPool =
                new ThreadPoolExecutor(this.config.getMaxJobEventWorker(), this.config.getMaxJobEventWorker(), 0,
                    TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(
                        "jobManagerEventProcess_worker"));

        masterDataRecoverWorker =
                new MasterDataRecoverWorker(config.getMasterName(), config.getTempStoreDataDir(), jobs, this.config);
        masterDataRecoverWorker.start();

        addJobsToPool();
        
        if (StringUtils.isNotEmpty(config.getZkServer()))
		{
			try
			{
				AnalyzerZKWatcher<MasterConfig> analyzerZKWatcher = 
						new AnalyzerZKWatcher<MasterConfig>(config);
				zk = new ZooKeeper(config.getZkServer(),3000,analyzerZKWatcher);
				analyzerZKWatcher.setZk(zk);
				
				ZKUtil.createGroupNodesIfNotExist(zk,config.getGroupId());
			}
			catch(Exception ex)
			{
				logger.error("zk init error!",ex);
			}
		}

        if (logger.isInfoEnabled())
            logger.info("jobManager init end, MaxJobEventWorker size : " + config.getMaxJobEventWorker());

    }


    @Override
    public void releaseResource() {
        stopped = true;

        try {
         // 导出所有结果，暂时不导出中间data，后面看是否需要
            //添加中间结果导出，不导出中间结果，会有部分数据丢失
            long start = System.currentTimeMillis();
            for(JobTask jobTask : this.jobTaskPool.values()) {
                while(JobTaskStatus.DOING.equals(jobTask.getStatus())) {
                    Thread.sleep(10000);
                    if(System.currentTimeMillis() - start > 60000)
                        break;
                }
            }
            
            if (jobs != null)
                for (Job j : jobs.values()) {
                    boolean gotIt = j.getTrunkLock().writeLock().tryLock();

                    if (gotIt) {
                        try {
                            if (!j.isMerged().get()) {
                                List<Map<String, Map<String, Object>>> mergeResults =
                                        new ArrayList<Map<String, Map<String, Object>>>();
                                new MergeJobOperation(j, 0, mergeResults, config, branchResultQueuePool.get(j
                                    .getJobName()), true).run();

                                j.isMerged().set(true);
                                logger.warn("job is timeout, last merge trunk success!");
                            }
                        }
                        finally {
                            j.getTrunkLock().writeLock().unlock();
                        }

                    }
                    JobDataOperation jobd =
                            new JobDataOperation(j, AnalysisConstants.JOBMANAGER_EVENT_EXPORTDATA, this.config);
                    jobd.run();
                    logger.info("releaseResouce now, export job : " + j.getJobName());
//                    while(!j.getTrunkExported().get())
//                        Thread.sleep(3000);
//                    if (!j.isExported().get()) {
//                        jobExporter.exportReport(j, false);
//                        logger.info("releaseResouce now, export job : " + j.getJobName());
//                    }
                }
            if (eventProcessThreadPool != null)
                eventProcessThreadPool.shutdown();

            if (masterDataRecoverWorker != null)
                masterDataRecoverWorker.stopWorker();
        } catch (Throwable e) {
            logger.error("error when stop the node", e);
        }
        finally {
            if (jobs != null)
                jobs.clear();

            if (jobTaskPool != null)
                jobTaskPool.clear();
            if(undoTaskQueue != null)
                undoTaskQueue.clear();

            if (statusPool != null)
                statusPool.clear();

            if (jobTaskResultsQueuePool != null)
                jobTaskResultsQueuePool.clear();

            if (branchResultQueuePool != null)
                branchResultQueuePool.clear();

            if (jobBuilder != null)
                jobBuilder.releaseResource();

            if (jobExporter != null)
                jobExporter.releaseResource();

            if (jobResultMerger != null)
                jobResultMerger.releaseResource();

            logger.info("jobManager releaseResource end");

        }
        
    }


    // 分配任务和结果提交处理由于是单线程处理，
    // 因此本身不用做状态池并发控制，将消耗较多的发送操作交给ServerConnector多线程操作
    @Override
    public void getUnDoJobTasks(GetTaskRequestEvent requestEvent) {

        String jobName = requestEvent.getJobName();
        int jobCount = requestEvent.getRequestJobCount();
        final List<JobTask> jobTasks = new ArrayList<JobTask>();

        //如果关闭，则直接返回一个空的JobTask的list给slave
        if(this.stopped) {
            masterNode.echoGetJobTasks(requestEvent.getSequence(), jobTasks, requestEvent.getChannel());
            return;
        }
        // 指定job
        if (jobName != null && jobs.containsKey(jobName)) {
            Job job = jobs.get(jobName);

            List<JobTask> tasks = job.getJobTasks();

            for (JobTask jobTask : tasks) {
                if (jobTask.getStatus().equals(JobTaskStatus.UNDO)) {
                    if (statusPool.replace(jobTask.getTaskId(), JobTaskStatus.UNDO, JobTaskStatus.DOING)) {
                        this.allocateTask(jobTask);
                        jobTasks.add(jobTask);

                        if (jobTasks.size() == jobCount)
                            break;
                    }
                }
            }
        }
        else {
            Iterator<JobTask> taskIter = undoTaskQueue.iterator();

            while (taskIter.hasNext()) {
//                String taskId = taskIds.next();
//                JobTask jobTask = jobTaskPool.get(taskId);
                JobTask jobTask = taskIter.next();
                if (!jobTaskPool.keySet().contains(jobTask.getTaskId())
                        || jobs.get(jobTask.getJobName()).getEpoch().get() > jobTask.getJobEpoch()
                        || jobs.get(jobTask.getJobName()).getJobTimeOut().get()) {
                    taskIter.remove();
                    continue;
                }
                
                if (jobs.get(jobTask.getJobName()).getJobConfig().getSlaveIpCondition() != null) {
                    try {
                        Channel channel = (Channel) requestEvent.getChannel();
                        if (!channel.getRemoteAddress().toString()
                            .matches(jobs.get(jobTask.getJobName()).getJobConfig().getSlaveIpCondition())) {
                            continue;
                        }
                    }
                    catch (Throwable e) {
                        logger.error(e);
                    }
                }

                if (statusPool.get(jobTask.getTaskId()).equals(JobTaskStatus.UNDO)) {
                    if (statusPool.replace(jobTask.getTaskId(), JobTaskStatus.UNDO, JobTaskStatus.DOING)) {
                        this.allocateTask(jobTask);
                        jobTasks.add(jobTask);
                        taskIter.remove();

                        if (jobTasks.size() >= jobCount)
                            break;
                    }
                } else 
                    taskIter.remove();
            }
        }

        // 是否需要用异步方式发送，减少对jobManager事件处理延时
        if (config.isUseAsynModeToSendResponse()) {
            final String sequence = requestEvent.getSequence();
            final Object channel = requestEvent.getChannel();

            // 由于该操作比较慢，开线程执行，保证速度
            eventProcessThreadPool.execute(new Runnable() {
                public void run() {
                    try {
                        masterNode.echoGetJobTasks(sequence, jobTasks, channel);
                    }
                    catch (Throwable e) {
                        logger.error(e);
                    }
                }
            });
        }
        else
            masterNode.echoGetJobTasks(requestEvent.getSequence(), jobTasks, requestEvent.getChannel());

    }


    private void allocateTask(JobTask jobTask) {
        jobTask.setStatus(JobTaskStatus.DOING);
        jobTask.setStartTime(System.currentTimeMillis());
    }


    // 分配任务和结果提交处理由于是单线程处理，
    // 因此本身不用做状态池并发控制，将消耗较多的发送操作交给ServerConnector多线程操作
    @Override
    public void addTaskResultToQueue(SendResultsRequestEvent jobResponseEvent) {

        JobTaskResult jobTaskResult = jobResponseEvent.getJobTaskResult();

        if (jobTaskResult.getTaskIds() != null && jobTaskResult.getTaskIds().size() > 0) {
            // 判断是否是过期的一些老任务数据，根据task和taskresult的createtime来判断
            // 以后要扩展成为如果发现当前的epoch < 结果的epoch，表明这台可能是从属的master，负责reduce，但是速度跟不上了
            if(jobTaskPool.get(jobTaskResult.getTaskIds().get(0)) == null) {
                logger.error("jobTask is null " + jobTaskResult.getTaskIds().get(0));
                masterNode.echoSendJobTaskResults(jobResponseEvent.getSequence(), "success", jobResponseEvent.getChannel());
                return;
            }
            if (jobTaskResult.getJobEpoch() != jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobEpoch() && this.config.getDispatchMaster()) {
            	
            	// 结果过期, 肯能是任务超时后, 被重新分配了
                if (jobTaskResult.getJobEpoch() < jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobEpoch()) {
                    logger.error("old task result will be discard! job:" + jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobName() + ",epoch:" + jobTaskResult.getJobEpoch() + ",slave:" + jobResponseEvent.getChannel());
                    masterNode.echoSendJobTaskResults(jobResponseEvent.getSequence(), "success", jobResponseEvent.getChannel());
                    return;
                }
                else {
                    // 给一定的容忍时间，暂时定为5秒
                    jobs.get(jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobName()).blockToResetJob(15000);
                    
                    // 这块有点疑问, 什么情况会出现
                    if (jobTaskResult.getJobEpoch() > jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobEpoch()) {
                        logger.error("otherMaster can't merge in time!job:" + jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobName() + ",taskResult epoch:" + jobTaskResult.getJobEpoch() + ", task epoch:" + jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobEpoch());
                        masterNode.echoSendJobTaskResults(jobResponseEvent.getSequence(), "success", jobResponseEvent.getChannel());
                        if(!this.config.getDispatchMaster()) {
                            jobs.get(jobTaskResult.getJobName()).reset(this);
                        } else {
                            return;
                        }
                    }
                }
            }

            if (logger.isWarnEnabled()) {
                StringBuilder ts =
                        new StringBuilder("Receive slave analysis result, jobTaskIds : ")
                            .append(jobTaskResult.toString()).append(", ").append(jobTaskResult.getTaskIds().size());
                logger.warn(ts.toString());
            }
            if(jobs.get(jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobName()).isMerged().get()) {
                masterNode.echoSendJobTaskResults(jobResponseEvent.getSequence(), "success", jobResponseEvent.getChannel());
                return;
            }

            // 先放入队列，防止小概率多线程并发问题
            jobTaskResultsQueuePool.get(jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobName()).offer(
                jobTaskResult);
            if(logger.isInfoEnabled()) {
                StringBuilder sb = new StringBuilder("add result [");
                for(String s : jobTaskResult.getTaskIds()) {
                    sb.append(s).append(",");
                }
                sb.append("] to queue:").append(jobTaskPool.get(jobTaskResult.getTaskIds().get(0)).getJobName());
                logger.info(sb.toString());
            }

            Iterator<String> iter = jobTaskResult.getTaskIds().iterator();
            while (iter.hasNext()) {
                String taskId = iter.next();
                JobTask jobTask = jobTaskPool.get(taskId);
                
                if (jobTask == null)
                {   	
                	logger.error(new StringBuilder("taskId :").append(taskId).append("not exist!").toString());
                	continue;
                }
                
                Job job = jobs.get(jobTask.getJobName());
                if(job == null) {
                    logger.error(new StringBuilder("job :").append(jobTask.getJobName()).append("not exist!").toString());
                    continue;
                }

                if (statusPool.replace(taskId, JobTaskStatus.DOING, JobTaskStatus.DONE)
                        || statusPool.replace(taskId, JobTaskStatus.UNDO, JobTaskStatus.DONE)) {
                    logger.info("task " + jobTask.getJobName() + " of job " + job.getJobName() + " done");
                    jobTask.setStatus(JobTaskStatus.DONE);
                    jobTask.getTailCursor().compareAndSet(true, false);
                    jobTask.setEndTime(System.currentTimeMillis());
                    jobTask.setLastMergedEpoch(job.getEpoch().get());
                    job.getCompletedTaskCount().incrementAndGet();
                } else {
                    if(!this.config.getDispatchMaster()) {
                        jobTask.setStatus(JobTaskStatus.DONE);
                        jobTask.getTailCursor().compareAndSet(true, false);
                        jobTask.setEndTime(System.currentTimeMillis());
                        jobTask.setLastMergedEpoch(job.getEpoch().get());
                        statusPool.put(taskId, JobTaskStatus.DONE);
                        iter.remove();
                    }
                }
                
                //对jobTask的执行结果打点
                StringBuilder log = new StringBuilder(ReportUtil.SLAVE_LOG).append(",timeStamp=")
                					.append(System.currentTimeMillis()).append(",epoch=")
                					.append(job.getEpoch()).append(",jobName=");
                log.append(jobTask.getJobName()).append(",taskId=")
                	.append(jobTask.getTaskId()).append(",recycleCounter=")
                	.append(jobTask.getRecycleCounter().get()).append(",slaveIp=")
                	.append(jobTaskResult.getSlaveIp()).append(",efficiency=")
                	.append(jobTaskResult.getEfficiency()).append(",");
               
                JobTaskExecuteInfo executeInfo = jobTaskResult.getTaskExecuteInfos().get(jobTask.getTaskId());
                
                if (executeInfo != null) {
                    log.append("analysisConsume=").append(executeInfo.getAnalysisConsume()).append(",")
                        .append("jobDataSize=").append(executeInfo.getJobDataSize()).append(",").append("totalLine=")
                        .append(executeInfo.getTotalLine()).append(",").append("errorLine=")
                        .append(executeInfo.getErrorLine()).append(",").append("emptyLine=")
                        .append(executeInfo.getEmptyLine()).append(",fileBegin=").append(executeInfo.getFileBegin())
                        .append(",fileLength=").append(executeInfo.getFileLength());
                    if(jobTask.getInput().startsWith("hub:")) {
                        jobTask.setJobSourceTimeStamp(executeInfo.getTimestamp());
                        job.updateCursor(jobTask.getUrl(), executeInfo.getFileBegin(), executeInfo.getFileLength(), executeInfo.getTimestamp());
                    }
                }
                else
                	logger.error(new StringBuilder().append("taskId : ").
                			append(jobTask.getTaskId()).append(" executeInfo is null!").toString());
                
                ReportUtil.clusterLog(log.toString());
                
                
                //增加一块对于zookeeper的支持
        		if (StringUtils.isNotEmpty(config.getZkServer()) && zk != null)
        		{
        			try
        			{     				
        				ZKUtil.updateOrCreateNode(zk,new StringBuilder()
        							.append(ZKUtil.getGroupMasterZKPath(config.getGroupId()))
        							.append("/").append(config.getMasterName())
        							.append("/runtime/").append(job.getEpoch())
        							.append("/").append(jobTask.getJobName())
        							.append("/").append(jobTask.getTaskId()).toString(),log.toString().getBytes("UTF-8"));
        				
        			}
        			catch(Exception ex)
        			{
        				logger.error("log to zk error!",ex);
        			}
        			
        		}
                
            }

        }

        // 是否需要用异步方式发送，减少对jobManager事件处理延时
        if (config.isUseAsynModeToSendResponse()) {
            final String sequence = jobResponseEvent.getSequence();
            final Object channel = jobResponseEvent.getChannel();

            eventProcessThreadPool.execute(new Runnable() {
                public void run() {
                    try {
                        masterNode.echoSendJobTaskResults(sequence, "success", channel);
                    }
                    catch (Throwable e) {
                        logger.error(e);
                    }
                }
            });
        }
        else
            masterNode.echoSendJobTaskResults(jobResponseEvent.getSequence(), "success", jobResponseEvent.getChannel());
    }


    @Override
    public void exportJobData(String jobName) {

        if (jobs.containsKey(jobName)) {
            jobExporter.exportEntryData(jobs.get(jobName));
        }
        else {
            logger.error("exportJobData do nothing, jobName " + jobName + " not exist!");
        }

    }


    @Override
    public void loadJobData(String jobName) {
        if (jobs.containsKey(jobName)) {
            jobExporter.loadEntryData(jobs.get(jobName));
        }
        else {
            logger.error("exportJobData do nothing, jobName " + jobName + " not exist!");
        }
    }


    /**
     * 从某一个备份载入job的临时数据开始恢复
     * 
     * @param jobName
     * @param epoch
     */
    @Override
    public void loadJobBackupData(String jobName, String bckPrefix) {
        if (jobs.containsKey(jobName)) {
            jobExporter.loadJobBackupData(jobs.get(jobName), bckPrefix);
        }
        else {
            logger.error("loadJobBackupData do nothing, jobName " + jobName + " not exist!");
        }
    }


    @Override
    public void loadJobDataToTmp(String jobName) {
        if (jobs.containsKey(jobName)) {
            jobExporter.loadEntryDataToTmp(jobs.get(jobName));
        }
        else {
            logger.error("exportJobData do nothing, jobName " + jobName + " not exist!");
        }
    }


    @Override
    public void clearJobData(String jobName) {

        Job job = jobs.get(jobName);

        if (job != null) {
            job.getJobResult().clear();

            if (logger.isWarnEnabled())
                logger.warn("clear job :" + job.getJobName() + " data.");
        }
    }


    @Override
    public synchronized void checkJobStatus() throws AnalysisException {

        // 通过外部事件激发重新载入配置
        if (jobBuilder.isNeedRebuild()) {
            if(logger.isInfoEnabled()) {
                logger.info("check job status need to rebuild");
            }
            jobs = jobBuilder.rebuild(jobs);

            if (jobs == null || (jobs != null && jobs.size() == 0))
                throw new AnalysisException("jobs should not be empty!");
        }

        try {
            if(this.config.getDispatchMaster())
                checkTaskStatus();
        } catch (Throwable e) {
            logger.error("checkTaskStatus Error", e);
        }
        
        // 合并任务,并导出报表
        try {
            mergeAndExportJobs();
        } catch (Throwable e) {
            logger.error("mergeAndExport Error", e);
        }
        
        //任务全部完成并且没有新加任务的情况下，休息1s
        for(Job job : jobs.values()) {
            if(!job.isExported().get() || job.getRebuildTag() == 2) {
                return;
            } else {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    logger.error(e);
                }
            }
        }
        
        // 打点观察Direct Memory区域的大小
        try {
            Class<?> c = Class.forName("java.nio.Bits");
            Field maxMemory = c.getDeclaredField("maxMemory");
            maxMemory.setAccessible(true);
            Field reservedMemory = c.getDeclaredField("reservedMemory");
            reservedMemory.setAccessible(true);
            synchronized (c) {
                Long maxMemoryValue = (Long) maxMemory.get(null);
                Long reservedMemoryValue = (Long) reservedMemory.get(null);
                if (logger.isInfoEnabled()) {
                    logger.info("now the maxMemory is " + String.valueOf(maxMemoryValue)
                            + " and the reservedMemory is " + String.valueOf(reservedMemoryValue));
                }
            }
        }
        catch (Throwable e) {
            logger.error("trying to get java.nio.Bits class failed");
        }

    }


    // 重新增加任务到任务池中
    protected void addJobsToPool() {
        for (Job job : jobs.values()) {
            List<JobTask> tasks = job.getJobTasks();

            for (JobTask task : tasks) {
                jobTaskPool.put(task.getTaskId(), task);
                statusPool.put(task.getTaskId(), task.getStatus());
                undoTaskQueue.offer(task);
            }
            
            if(jobTaskResultsQueuePool.get(job.getJobName()) == null)
                jobTaskResultsQueuePool.put(job.getJobName(), new LinkedBlockingQueue<JobTaskResult>());
            if(branchResultQueuePool.get(job.getJobName()) == null)
                branchResultQueuePool.put(job.getJobName(), new LinkedBlockingQueue<JobMergedResult>());
        }

    }


    // 做合并和导出，重置任务的检查操作
    //所有任务一起来轮询，对Master来讲，有点资源浪费
    //可以通过以下几种方式改进：
    //1、针对job的属性设置监听器，Listener模式
    //2、使用Observer模式
    protected void mergeAndExportJobs() {
        Iterator<Map.Entry<String, Job>> iter = jobs.entrySet().iterator();
        while(iter.hasNext()) {
        	Job job = iter.next().getValue();
        	if(job.getRebuildTag() == 2) {
        	    job.rebuild(0, null, this);
        	    continue;
        	}
        	if (!job.getJobTimeOut().get())
        	{
		        // 需要合并该job的task
		        if (!job.isMerging().get() && job.needMerge()) {
		            logger.warn("job " + job.getJobName()
		                 + " complete tasks:" + job.getCompletedTaskCount().get() + ", merged tasks :" + job.getMergedTaskCount().get());
		            final Job j = job;
		            final BlockingQueue<JobMergedResult> branchResultQueue = branchResultQueuePool.get(j.getJobName());
		            final BlockingQueue<JobTaskResult> jobTaskResultsQueue = jobTaskResultsQueuePool.get(j.getJobName());
		
		            if (j.isMerging().compareAndSet(false, true))
		                eventProcessThreadPool.execute(new Runnable() {
		                    public void run() {
		                        try {
		                            jobResultMerger.merge(j, branchResultQueue, jobTaskResultsQueue, true);
		                        } catch (Throwable e) {
		                            logger.error(e);
		                        }
		                        finally {
		                            j.isMerging().set(false);
		                        }
		                    }
		                });
		        }
        	}
        	else
        	{
        		// Job超时了, 尝试做一次主干merge
        		//判断是否还有和主干合并的线程，如果没有可以设置完成标识
        		boolean gotIt = job.getTrunkLock().writeLock().tryLock();
        		
        		if (gotIt)
        		{
        			try
        			{	if(!job.isMerged().get())
        				{
        					List<Map<String, Map<String, Object>>> mergeResults = new ArrayList<Map<String, Map<String, Object>>>();
        					new MergeJobOperation(job,0,mergeResults,config,branchResultQueuePool.get(job.getJobName())).run();
        				
        					job.isMerged().set(true);
        					logger.warn("job is timeout, last merge trunk success!");
        				}
        			}
        			finally
        			{
        				job.getTrunkLock().writeLock().unlock();
        			}
        			
        		}
        		
        	}

            // 需要导出该job的数据
            if (!job.isExporting().get() && job.needExport()) {
                final Job j = job;

                if (j.isExporting().compareAndSet(false, true))
                    eventProcessThreadPool.execute(new Runnable() {
                        public void run() {
                            try {
                                // 虽然是多线程，但还是阻塞模式来做
                                jobExporter.exportReport(j, false);
                                j.isExported().set(true);
                            } catch (Throwable e) {
                                logger.error(e);
                            }
                            finally {
                                j.isExporting().set(false);
                            }

                            // 判断是否需要开始导出中间结果,放在外部不妨碍下一次的处理
                            exportOrCleanTrunk(j);
                        }
                    });
            }
            
            //做一次任务处理时间判断，如果超时将设置job的超时状态位置
            if(this.config.getDispatchMaster())
                job.checkJobTimeOut();

            // 任务是否需要被重置
            if (job.needReset() || (!this.config.getDispatchMaster() && job.isExported().get()) ) {
                if(logger.isWarnEnabled())
                    logger.warn("job " + job.getJobName() + " be reset now.");
                
                //检查任务是否需要重新build
                if(job.getRebuildTag() == -1) {
                    job.rebuild(0, null, this);
                    iter.remove();
                }
                if(job.getRebuildTag() == 1) {
                    job.rebuild(0, null, this);
                }
                
            	StringBuilder sb = new StringBuilder(ReportUtil.MASTER_LOG).append(",timeStamp=")
            							.append(System.currentTimeMillis()).append(",epoch=");
            	sb.append(job.getEpoch()).append(",jobName=")
            		.append(job.getJobName()).append(",timeConsume=")
            		.append(System.currentTimeMillis() - job.getStartTime()).append(",jobMergeTime=")
            		.append(job.getJobMergeTime().get()).append(",jobExportTime=")
            		.append(job.getJobExportTime()).append(",taskCount=")
            		.append(job.getTaskCount()).append(",completedTaskCount=")
            		.append(job.getCompletedTaskCount().get()).append(",mergedTaskCount=")
            		.append(job.getMergedTaskCount().get()).append(",jobMergeBranchCount=")
            		.append(job.getJobMergeBranchCount().get());
            	ReportUtil.clusterLog(sb.toString());
                
            	//增加一块对于zookeeper的支持
        		if (StringUtils.isNotEmpty(config.getZkServer()) && zk != null)
        		{
        			try
        			{     				
        				ZKUtil.updateOrCreateNode(zk,new StringBuilder()
        							.append(ZKUtil.getGroupMasterZKPath(config.getGroupId()))
        							.append("/").append(config.getMasterName())
        							.append("/runtime/").append(job.getEpoch())
        							.append("/").append(job.getJobName()).toString(),sb.toString().getBytes("UTF-8"));
        				
        			}
        			catch(Exception ex)
        			{
        				logger.error("log to zk error!",ex);
        			}
        			
        		}
            	

                job.reset(this);
                
                if (logger.isInfoEnabled()) {
                    sb = new StringBuilder("jobManager:{jobs:").append(jobs.size()).append(
                                ",jobTaskPool:" + jobTaskPool.size());
                    sb.append(",statusPool:").append(statusPool.size()).append(",undoTasks:")
                        .append(undoTaskQueue.size()).append("}");
                    logger.info(sb.toString());
                }

                List<JobTask> tasks = job.getJobTasks();

                for (JobTask task : tasks) {
                    statusPool.put(task.getTaskId(), task.getStatus());
                }
            }
        }
    }


    /**
     * 在导出数据以后，判断是否需要清空主干，是否需要导出主干
     * 
     * @param job
     */
    protected void exportOrCleanTrunk(Job job) {
        boolean needToSetJobResultNull = false;

        // 判断是否到了报表的有效时间段，支持小时，日，月三种方式
        if (job.getJobConfig().getReportPeriodDefine().equals(AnalysisConstants.REPORT_PERIOD_DAY)) {
            Calendar calendar = Calendar.getInstance();
            int now = calendar.get(Calendar.DAY_OF_MONTH);

            if (job.getReportPeriodFlag() != -1 && now != job.getReportPeriodFlag())
                needToSetJobResultNull = true;

            job.setReportPeriodFlag(now);
        }
        else {
            if (job.getJobConfig().getReportPeriodDefine().equals(AnalysisConstants.REPORT_PERIOD_HOUR)) {
                Calendar calendar = Calendar.getInstance();
                int now = calendar.get(Calendar.HOUR_OF_DAY);

                if (job.getReportPeriodFlag() != -1 && now != job.getReportPeriodFlag())
                    needToSetJobResultNull = true;

                job.setReportPeriodFlag(now);
            }
            else {
                if (job.getJobConfig().getReportPeriodDefine().equals(AnalysisConstants.REPORT_PERIOD_MONTH)) {
                    Calendar calendar = Calendar.getInstance();
                    int now = calendar.get(Calendar.MONTH);

                    if (job.getReportPeriodFlag() != -1 && now != job.getReportPeriodFlag())
                        needToSetJobResultNull = true;

                    job.setReportPeriodFlag(now);
                }
            }
        }

        if (needToSetJobResultNull) {
            job.setJobResult(null);
            job.getEpoch().set(0);

            // 删除临时文件，防止重复载入使得清空不生效
            if (config.getSaveTmpResultToFile()) {
                JobDataOperation jobDataOperation =
                        new JobDataOperation(job, AnalysisConstants.JOBMANAGER_EVENT_DEL_DATAFILE, this.config);
                jobDataOperation.run();
            }
            
            if(logger.isWarnEnabled())
                logger.warn("job " + job.getJobName() + " report data be reset.it's a new start. ");
        }

        // 清除主干数据，到时候自然会载入
        if (config.getSaveTmpResultToFile() && (job.getJobConfig().getSaveTmpResultToFile() == null || job.getJobConfig().getSaveTmpResultToFile())) {
            logger.warn("@disk2Mem mode: start " + job.getJobName() + " store trunk to disk now .");

            JobDataOperation jobDataOperation =
                    new JobDataOperation(job, AnalysisConstants.JOBMANAGER_EVENT_SETNULL_EXPORTDATA, this.config);
            jobDataOperation.run();

        }
        else {
            if (job.getLastExportTime() == 0
                    || System.currentTimeMillis() - job.getLastExportTime() >= config.getExportInterval() || stopped) {
                logger.warn("export job: " + job.getJobName() + " trunk to disk.");

                JobDataOperation jobDataOperation =
                        new JobDataOperation(job, AnalysisConstants.JOBMANAGER_EVENT_EXPORTDATA, this.config);
                jobDataOperation.run();
            }
        }
    }


    // 重置在指定时间内未完成的任务
    protected void checkTaskStatus() {
        Iterator<String> taskIds = statusPool.keySet().iterator();

        while (taskIds.hasNext()) {
            String taskId = taskIds.next();

            JobTaskStatus taskStatus = statusPool.get(taskId);
            JobTask jobTask = jobTaskPool.get(taskId);

            if (taskStatus == JobTaskStatus.DOING && jobTask.getStartTime() != 0
                    && System.currentTimeMillis() - jobTask.getStartTime() >= jobTask.getTaskRecycleTime() * 1000) {
                if (statusPool.replace(taskId, JobTaskStatus.DOING, JobTaskStatus.UNDO)) {
                    jobTask.setStatus(JobTaskStatus.UNDO);
                    undoTaskQueue.offer(jobTask);
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
    public Map<String, Job> getJobs() {
        return jobs;
    }


    @Override
    public void setJobs(Map<String, Job> jobs) {
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


    public void setJobTaskResultsQueuePool(Map<String, BlockingQueue<JobTaskResult>> jobTaskResultsQueuePool) {
        this.jobTaskResultsQueuePool = jobTaskResultsQueuePool;
    }


    public Map<String, BlockingQueue<JobMergedResult>> getBranchResultQueuePool() {
        return branchResultQueuePool;
    }


    public void setBranchResultQueuePool(Map<String, BlockingQueue<JobMergedResult>> branchResultQueuePool) {
        this.branchResultQueuePool = branchResultQueuePool;
    }


    /**
     * @return the jobTaskPool
     */
    public ConcurrentMap<String, JobTask> getJobTaskPool() {
        return jobTaskPool;
    }


    /**
     * @return the statusPool
     */
    public ConcurrentMap<String, JobTaskStatus> getStatusPool() {
        return statusPool;
    }


    /**
     * @return the undoTaskQueue
     */
    public BlockingQueue<JobTask> getUndoTaskQueue() {
        return undoTaskQueue;
    }

}
