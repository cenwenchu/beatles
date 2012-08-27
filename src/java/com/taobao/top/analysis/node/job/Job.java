package com.taobao.top.analysis.node.job;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.config.JobConfig;
import com.taobao.top.analysis.node.component.JobManager;
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
	//job的配置信息
	JobConfig jobConfig;
	//业务分析规则解析后的内容，会被传递到slave
	Rule statisticsRule;
	//job下的子任务
	List<JobTask> jobTasks;
	//任务总数
	int taskCount = 0;
	//完成的任务数
	AtomicInteger completedTaskCount;
	//合并的任务数
	AtomicInteger mergedTaskCount;
	//是否需要载入结果文件从磁盘，是开启了磁盘换内存才会有用
	AtomicBoolean needLoadResultFile;
	//job的创建时间
	long startTime;
	//防止大量写日志的阀
	transient Threshold threshold;
	//主干读写锁，控制并发
	ReentrantReadWriteLock trunkLock;
	//载入的锁，防止重复载入
	ReentrantLock loadLock;
	
	//是否正在合并检查和处理，只允许一个线程做
	AtomicBoolean merging;
	//是否正在导出检查和处理，只允许一个线程做
	AtomicBoolean exporting;
	//是否合并完毕，可以进入导出阶段
	AtomicBoolean merged;
	//是否导出完毕，可以被重置任务
	AtomicBoolean exported;
	//是否由于超过2倍的任务时间导致需要重置任务
	AtomicBoolean jobTimeOut;
	
	//最后一次导出临时文件的时间，用于在非磁盘换空间的模式下，固定一段时间导出作为容灾，时间间隔参考masterconfig 的exportInterval
	long lastExportTime;
	
	//用于纪录报表最近一次的时间戳，比较是否要重置报表
	int reportPeriodFlag;
	
	//用于协同多个master的情况
	private ReentrantLock waitlock;
	private Condition waitToJobReset;
	
	/**
	 * job用于执行merge的时间，只纪录主干merge的时间
	 */
	private AtomicLong jobMergeTime;
	
	/**
	 * job分支合并的次数
	 */
	private AtomicInteger jobMergeBranchCount;
	
	/**
	 * job导出所用的时间
	 */
	private long jobExportTime;
	
	
	/**
	 * 处理后的结果池，key是entry的id， value是Map(key是entry定义的key组合,value是统计后的结果)
	 * 采用线程不安全，只有单线程操作此结果集
	 */
	private Map<String, Map<String, Object>> jobResult;
	
	/**
	 * 被交换到磁盘上的结果集
	 */
	private Map<String, Map<String, Object>> diskResult;
	
	/**
	 * 任务被重复执行了多少次，标识任务已经生存到了多少代，当跨过数据外置的时候，清零
	 */
	private AtomicInteger epoch;
	
	/**
	 * 用于系统恢复的时候从临时备份数据中读取临时数据在日志获取端的游标，以时间戳作为游标
	 * 简单来说就是每一个master备份出去的临时文件包含当前分析的数据内容和这些数据对应到数据源（日志产生方）的日志拖拉绝对游标
	 * 这个参数将会配合epoch一起加入到job的task的input中作为参数传递，epoch＝1的时候才会判断是否要根据这个参数重置游标
	 */
	private long jobSourceTimeStamp = 0;
	
	/**
	 * 任务重构标志位
	 * 0：默认不重构
	 * 1：重构
	 * -1：删除
	 */
	private transient int rebuildTag = 0;
	
	/**
	 * 重构任务，默认为空
	 */
	private transient Job rebuildJob = null;
	
	/**
	 * 是否已经export主干到文件标记
	 */
	private transient AtomicBoolean trunkExported;
	
	/**
	 * 是否已经merge磁盘文件
	 */
	private transient boolean diskResultMerged;
	
	/**
     * 游标管理的Map，采用hub拉取日志方式，使用该Map存储各数据来源游标位置
     * 采用Hub方式的标识是HTTP的URL为hub://开头
     */
    private ConcurrentMap<String, Long> cursorMap;
    
    /**
     * 游标管理的临时Map，采用hub拉取日志方式，使用该Map存储各数据来源游标位置
     * 采用Hub方式的标识是HTTP的URL为hub://开头
     */
    private ConcurrentMap<String, Long> tmpMap;
	
    /**
     * 采用hub拉取日志时，使用该timestampMap记录日志上次拉取的时间，
     * 用于做跨天处理的判断，如果跨天就多拉一次日志
     */
    private ConcurrentMap<String, Long> timestampMap;

	public Job()
	{
		jobTasks = new ArrayList<JobTask>();
		threshold = new Threshold(5000);
		trunkLock = new ReentrantReadWriteLock();
		loadLock = new ReentrantLock();
		waitlock = new ReentrantLock();
		waitToJobReset = waitlock.newCondition();
		merged = new AtomicBoolean(false);
        merging = new AtomicBoolean(false);
        exporting = new AtomicBoolean(false);
        exported = new AtomicBoolean(false);
		
		lastExportTime = 0;
		reportPeriodFlag = -1;
		epoch = new AtomicInteger(0);
		trunkExported = new AtomicBoolean(false);
		diskResultMerged = false;
		cursorMap = new ConcurrentHashMap<String, Long>();
		tmpMap = new ConcurrentHashMap<String, Long>();
		timestampMap = new ConcurrentHashMap<String, Long>();
//		reset(null);
	}
	
	public void notifySomeWaitResetJob()
	{
		boolean flag = false;
		
		try
		{
			flag = waitlock.tryLock(50,TimeUnit.MILLISECONDS);
			
			if (flag)
				waitToJobReset.signalAll();
		}
		catch(InterruptedException ie)
		{
			//do nothing;
		}
		catch(Exception ex)
		{
			logger.error(ex);
		}
		finally
		{
			if (flag)
				waitlock.unlock();
		}
	}
	
	public void blockToResetJob(long waittime)
	{
		if (waittime < 0)
			return;
			
		boolean flag = waitlock.tryLock();
		
		if (flag)
		{
			try
			{
				waitToJobReset.await(waittime, TimeUnit.MILLISECONDS);
			}
			catch(InterruptedException ie)
			{
				//do nothing;
			}
			catch(Exception ex)
			{
				logger.error("blockToResetJob error.",ex);
			}
			finally
			{
				waitlock.unlock();
			}
		}
			
	}
	
	public long getJobSourceTimeStamp() {
		return jobSourceTimeStamp;
	}

	public void setJobSourceTimeStamp(long jobSourceTimeStamp) {
		this.jobSourceTimeStamp = jobSourceTimeStamp;
		for(JobTask task : jobTasks)
        {
		    if(task.getUrl().startsWith("http://"))
		        task.setJobSourceTimeStamp(jobSourceTimeStamp);
        }
	}

	public AtomicBoolean getJobTimeOut() {
		return jobTimeOut;
	}

	public void setJobTimeOut(AtomicBoolean jobTimeOut) {
		this.jobTimeOut = jobTimeOut;
	}

	public AtomicInteger getEpoch() {
		return epoch;
	}

	public void setEpoch(AtomicInteger epoch) {
		this.epoch = epoch;
	}

	public AtomicLong getJobMergeTime() {
		return jobMergeTime;
	}

	public AtomicInteger getJobMergeBranchCount() {
		return jobMergeBranchCount;
	}

	public ReentrantLock getLoadLock() {
		return loadLock;
	}


	public void setLoadLock(ReentrantLock loadLock) {
		this.loadLock = loadLock;
	}


	public ReentrantReadWriteLock getTrunkLock() {
		return trunkLock;
	}


	public void setTrunkLock(ReentrantReadWriteLock trunkLock) {
		this.trunkLock = trunkLock;
	}

	public boolean needMerge()
	{
		return !merged.get() && completedTaskCount.get() > mergedTaskCount.get();
	}
	
	public boolean needExport()
	{
		return !exported.get() && merged.get() && (mergedTaskCount.get() >= taskCount || jobTimeOut.get());
	}
	
	public void checkJobTimeOut()
	{
		long consume = System.currentTimeMillis() - startTime;
		
		if (mergedTaskCount.get() < taskCount && consume > jobConfig.getJobResetTime() * 1000) {
			if (logger.isWarnEnabled() && !threshold.sholdBlock())
				logger.warn("job : " + jobName + " can't complete in time!" + " mergedTaskCount:" + mergedTaskCount.get() + ",taskCount:" + taskCount
                    + ",taskList:" + jobTasks.size());
		}
		
		if (consume > jobConfig.getJobResetTime() * 1000 * 2)
			this.jobTimeOut.set(true);
		
	}

	public boolean needReset()
	{
		long consume = System.currentTimeMillis() - startTime;
		
		if(logger.isInfoEnabled() && !threshold.sholdBlock() && (consume >= jobConfig.getJobResetTime() * 1000)) {
		    logger.info("job:" + jobName + "," + exported.get());
		}
				
		if (exported.get() && (consume >= jobConfig.getJobResetTime() * 1000))
			return true;
		
		return false;
	}
	
	
	public long getStartTime() {
		return startTime;
	}

	public void reset(JobManager jobManager)
	{
		epoch.incrementAndGet();
		tmpMap.clear();
		
		for(JobTask task : jobTasks)
		{
			task.setStatus(JobTaskStatus.UNDO);
			task.setCreatTime(System.currentTimeMillis());
			task.setStartTime(0);
			task.getRecycleCounter().set(0);
			task.setJobEpoch(epoch.get());
			long start = 0;
			if(task.getInput().startsWith("hub://")) {
			    String key = task.getInput().substring(0, task.getInput().indexOf('?'));
			    if(timestampMap.containsKey(key))
			        task.setJobSourceTimeStamp(timestampMap.get(key));
			    if(!tmpMap.containsKey(key)) {
			        logger.info("get job cursor " + key + ", " + cursorMap.get(key));
			        tmpMap.put(key, cursorMap.get(key));
			    }
			    start = tmpMap.get(key);
			    tmpMap.put(key, start + jobConfig.getHubCursorStep());
			    logger.info("dispatch job cursor " + key + "," + start + ", " + task.getJobSourceTimeStamp());
			}
			task.resetInput(start, jobConfig.getHubCursorStep());
		}
			
		taskCount = jobTasks.size();
		
		completedTaskCount = new AtomicInteger(0);
		mergedTaskCount = new AtomicInteger(0);
		needLoadResultFile = new AtomicBoolean(true);
		startTime = System.currentTimeMillis();
		merged = new AtomicBoolean(false);
		merging = new AtomicBoolean(false);
		exporting = new AtomicBoolean(false);
		exported = new AtomicBoolean(false);
		jobTimeOut = new AtomicBoolean(false);
		trunkExported = new AtomicBoolean(false);
		diskResultMerged = false;
		jobMergeTime = new AtomicLong(0);
		jobExportTime = 0;
		jobMergeBranchCount = new AtomicInteger(0);
		diskResult = null;
		StringBuilder bs = new StringBuilder();
        for(JobTask jobTask : this.jobTasks) {
            bs.append(jobTask.getUrl());
            bs.append(";");
        }
        logger.info("jobTasks:[" + bs.toString() + " ]");
		
		notifySomeWaitResetJob();
		
		if(jobManager != null)
		    jobManager.getUndoTaskQueue().addAll(jobTasks);
		
	}
	

	public AtomicBoolean getNeedLoadResultFile() {
		return needLoadResultFile;
	}


	public void setNeedLoadResultFile(AtomicBoolean needLoadResultFile) {
		this.needLoadResultFile = needLoadResultFile;
	}


	public long getJobExportTime() {
		return jobExportTime;
	}

	public void setJobExportTime(long jobExportTime) {
		this.jobExportTime = jobExportTime;
	}

	public Map<String, Map<String, Object>> getJobResult() {
		return jobResult;
	}

	public void setJobResult(Map<String, Map<String, Object>> jobResult) {
		this.jobResult = jobResult;
	}
	
	public AtomicBoolean isMerged() {
		return merged;
	}


	public AtomicBoolean isMerging() {
		return merging;
	}


	public AtomicBoolean isExporting() {
		return exporting;
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
	
	public void addTaskCount()
	{
		this.taskCount += 1;
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


	public AtomicBoolean isExported() {
		return exported;
	}


	public Map<String, Map<String, Object>> getDiskResult() {
		return diskResult;
	}


	public void setDiskResult(Map<String, Map<String, Object>> diskResult) {
		this.diskResult = diskResult;
	}


	public long getLastExportTime() {
		return lastExportTime;
	}


	public void setLastExportTime(long lastExportTime) {
		this.lastExportTime = lastExportTime;
	}


	public int getReportPeriodFlag() {
		return reportPeriodFlag;
	}


	public void setReportPeriodFlag(int reportPeriodFlag) {
		this.reportPeriodFlag = reportPeriodFlag;
	}
	

	/**
	 * 这里的逻辑是第一次调用该方法，置标记位
	 * 第二次调用，真正rebuild
	 * 标记重构，保证在任务执行结束后再重构，无缝重载
	 * 采用这种逻辑的目的是入口统一
	 * 此处代码可用其他结构实现，可以从设计模式及性能上面考虑
	 * @param job
	 */
    public void rebuild(int rebuildTag, Job job, JobManager jobManager) {
        if (job != null)
            this.rebuildJob = job;
        //第一次进入置标记位
        if (this.rebuildTag == 0 || this.rebuildJob == null || jobManager == null) {
            this.rebuildTag = rebuildTag;
            return;
        }
        
        //第二次进入删除job
        if (this.rebuildTag == -1) {
            logger.error("delete job:" + this.jobName);
            for (JobTask jobTask : this.jobTasks) {
                jobManager.getJobTaskPool().remove(jobTask.getTaskId());
                jobManager.getStatusPool().remove(jobTask.getTaskId());
            }
            jobManager.getBranchResultQueuePool().remove(this.jobName);
            jobManager.getJobTaskResultsQueuePool().remove(this.jobName);
            this.rebuildTag = rebuildTag;
            this.rebuildJob = null;
            return;
        }
        //第二次进入添加job
        if(this.rebuildTag == 2) {
            logger.error("add job:" + this.jobName);
            for (JobTask jobTask : this.jobTasks) {
                jobManager.getJobTaskPool().put(jobTask.getTaskId(), jobTask);
                jobManager.getStatusPool().put(jobTask.getTaskId(), jobTask.getStatus());
            }
            if(jobManager.getBranchResultQueuePool().get(this.jobName) == null)
                jobManager.getBranchResultQueuePool().put(this.jobName, new LinkedBlockingQueue<JobMergedResult>());
            if(jobManager.getJobTaskResultsQueuePool().get(this.jobName) == null)
                jobManager.getJobTaskResultsQueuePool().put(this.jobName, new LinkedBlockingQueue<JobTaskResult>());
            this.rebuildTag = rebuildTag;
            this.rebuildJob = null;
            this.reset(jobManager);
            return;
        }

        //第二次进入修改job
        this.jobConfig = rebuildJob.getJobConfig();
        this.statisticsRule = rebuildJob.statisticsRule;
        int o = this.taskCount;
        this.taskCount = rebuildJob.taskCount;
        //更新cursorMap和timestampMap
        for(String key : rebuildJob.cursorMap.keySet()) {
            this.cursorMap.putIfAbsent(key, rebuildJob.cursorMap.get(key));
        }
        for(String key : rebuildJob.timestampMap.keySet()) {
            this.timestampMap.putIfAbsent(key, rebuildJob.timestampMap.get(key));
        }
        Iterator<Map.Entry<String, Long>> mapIter = this.cursorMap.entrySet().iterator();
        while(mapIter.hasNext()) {
            if(!rebuildJob.cursorMap.containsKey(mapIter.next().getKey()))
                mapIter.remove();
        }
        mapIter = this.timestampMap.entrySet().iterator();
        while(mapIter.hasNext()) {
            if(!rebuildJob.timestampMap.containsKey(mapIter.next().getKey()))
                mapIter.remove();
        }
        int i = 0, j = 0;
        for (JobTask jobTask : this.jobTasks) {
            Iterator<JobTask> iter = rebuildJob.jobTasks.iterator();
            while (iter.hasNext()) {
                JobTask task = iter.next();
                if (jobTask.getUrl() != null && jobTask.getUrl().equals(task.getUrl()) && jobTask.getRebuildStatus() != 1) {
                    jobTask.rebuild(task);
                    iter.remove();
                    i++;
                }
            }
        }
        Iterator<JobTask> iter = this.jobTasks.iterator();
        while (iter.hasNext()) {
            JobTask jobTask = iter.next();
            if (jobTask.getRebuildStatus() != 1) {
                iter.remove();
                jobManager.getJobTaskPool().remove(jobTask.getTaskId());
                jobManager.getStatusPool().remove(jobTask.getTaskId());
                j++;
            }
        }
        for (JobTask jobTask : rebuildJob.getJobTasks()) {
            this.jobTasks.add(jobTask);
        }
        for (JobTask jobTask : this.jobTasks) {
            jobTask.setRebuildStatus(0);
            jobManager.getJobTaskPool().put(jobTask.getTaskId(), jobTask);
            jobManager.getStatusPool().put(jobTask.getTaskId(), jobTask.getStatus());
        }
        logger.error(jobName + " oldJob:" + o + ",newJob:" + this.taskCount + ",update:" + i + ",delete:" + j + ",add:"
                + rebuildJob.getJobTasks().size());
        if(jobManager.getBranchResultQueuePool().get(this.jobName) == null)
            jobManager.getBranchResultQueuePool().put(this.jobName, new LinkedBlockingQueue<JobMergedResult>());
        if(jobManager.getJobTaskResultsQueuePool().get(this.jobName) == null)
            jobManager.getJobTaskResultsQueuePool().put(this.jobName, new LinkedBlockingQueue<JobTaskResult>());
        this.rebuildTag = rebuildTag;
        this.rebuildJob = null;
        
        StringBuilder bs = new StringBuilder();
        for(JobTask jobTask : this.jobTasks) {
            bs.append(jobTask.getUrl());
            bs.append(";");
        }
        logger.info("jobTasks:[" + bs.toString() + " ]");
    }
    
    /**
     * 更新游标
     * 游标的处理会引起误差
     * @param url
     * @param fileLength
     */
    public void updateCursor(String url, Long fileBegin, Long fileLength, Long timestamp) {
        if(url.indexOf('?') < 0)
            return;
        if(fileBegin == null || fileLength == null || timestamp == null)
            return;
        String key = url.substring(0, url.indexOf('?'));
        logger.info("updateCursor " + key + "," + fileBegin + "," + fileLength + "," + this.cursorMap.get(key) + ",timestamp:" + this.timestampMap.get(key));
        if(!this.timestampMap.containsKey(key) || this.timestampMap.get(key).compareTo(0L) == 0 ) {
            this.timestampMap.put(key, timestamp);
        }
        if(this.cursorMap.containsKey(key)) {
            long last = (this.timestampMap.get(key).longValue() + 28800000L) / (24 * 3600 * 1000);
            long now = (timestamp.longValue() + 28800000L) / (24 * 3600 * 1000);
            if(fileBegin.equals(0L) && !this.cursorMap.get(key).equals(0L) && (now > last || fileLength.compareTo(0L) > 0))
                this.cursorMap.put(key, fileLength);
            else
                this.cursorMap.put(key, this.cursorMap.get(key) + fileLength);
        }
        if(timestamp.compareTo(this.timestampMap.get(key)) > 0)
            this.timestampMap.put(key, timestamp);
        logger.info("updateCursor " + key + "," + fileBegin + "," + fileLength + "," + this.cursorMap.get(key) + ",timestamp:" + timestamp);
    }

    /**
     * @return the rebuildTag
     */
    public int getRebuildTag() {
        return rebuildTag;
    }

    /**
     * @return the trunkExported
     */
    public AtomicBoolean getTrunkExported() {
        return trunkExported;
    }

    /**
     * @return the diskResultMerged
     */
    public boolean isDiskResultMerged() {
        return diskResultMerged;
    }

    /**
     * @param diskResultMerged the diskResultMerged to set
     */
    public void setDiskResultMerged(boolean diskResultMerged) {
        this.diskResultMerged = diskResultMerged;
    }

    /**
     * @param rebuildTag the rebuildTag to set
     */
    public void setRebuildTag(int rebuildTag) {
        this.rebuildTag = rebuildTag;
    }

    /**
     * @return the cursorMap
     */
    public ConcurrentMap<String, Long> getCursorMap() {
        return cursorMap;
    }

    /**
     * @return the timestampMap
     */
    public ConcurrentMap<String, Long> getTimestampMap() {
        return timestampMap;
    }
}
