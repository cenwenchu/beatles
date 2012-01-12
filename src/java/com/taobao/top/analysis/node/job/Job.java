package com.taobao.top.analysis.node.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.config.JobConfig;
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
	Threshold threshold;
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
	
	//最后一次导出临时文件的时间，用于在非磁盘换空间的模式下，固定一段时间导出作为容灾，时间间隔参考masterconfig 的exportInterval
	long lastExportTime;
	
	//用于纪录报表最近一次的时间戳，比较是否要重置报表
	int reportPeriodFlag;
	
	
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
	
	public Job()
	{
		jobTasks = new ArrayList<JobTask>();
		threshold = new Threshold(5000);
		trunkLock = new ReentrantReadWriteLock();
		loadLock = new ReentrantLock();
		lastExportTime = 0;
		reportPeriodFlag = -1;
		epoch = new AtomicInteger(0);
		reset();
	}


	public AtomicInteger getEpoch() {
		return epoch;
	}



	public void setEpoch(AtomicInteger epoch) {
		this.epoch = epoch;
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
		return !exported.get() && merged.get() && mergedTaskCount.get() == taskCount;
	}

	public boolean needReset()
	{
		long consume = System.currentTimeMillis() - startTime;
				
		if ((exported.get() && (consume >= jobConfig.getJobResetTime() * 1000))
				||(consume > jobConfig.getJobResetTime() * 1000 * 2))
			return true;
		
		if (mergedTaskCount.get() < taskCount && consume > jobConfig.getJobResetTime() * 1000)
			if (logger.isWarnEnabled() && !threshold.sholdBlock())
				logger.warn("job : " + jobName + " can't complete in time!");
		
		return false;
	}
	
	public void reset()
	{
		epoch.incrementAndGet();
		
		for(JobTask task : jobTasks)
		{
			task.setStatus(JobTaskStatus.UNDO);
			task.setCreatTime(System.currentTimeMillis());
			task.setStartTime(0);
			task.getRecycleCounter().set(0);
			task.setJobEpoch(epoch.get());
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
		diskResult = null;
		
	}
	

	public AtomicBoolean getNeedLoadResultFile() {
		return needLoadResultFile;
	}


	public void setNeedLoadResultFile(AtomicBoolean needLoadResultFile) {
		this.needLoadResultFile = needLoadResultFile;
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
	

}
