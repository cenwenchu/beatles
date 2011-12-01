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
	JobConfig jobConfig;
	Rule statisticsRule;
	List<JobTask> jobTasks;
	int taskCount = 0;
	AtomicInteger completedTaskCount;
	AtomicInteger mergedTaskCount;
	AtomicBoolean needLoadResultFile;
	long startTime;
	Threshold threshold;
	ReentrantReadWriteLock trunkLock;
	ReentrantLock loadLock;
	
	//一个job只需要一个线程负责merge和export，由于外部jobmanager是单线程，这里直接采用非原子操作
	boolean merging = false;
	boolean exporting = false;
	boolean merged = false;
	boolean exported = false;
	
	
	/**
	 * 处理后的结果池，key是entry的id， value是Map(key是entry定义的key组合,value是统计后的结果)
	 * 采用线程不安全，只有单线程操作此结果集
	 */
	private Map<String, Map<String, Object>> jobResult;
	
	/**
	 * 被交换到磁盘上的结果集
	 */
	private Map<String, Map<String, Object>> diskResult;
	
	public Job()
	{
		jobTasks = new ArrayList<JobTask>();
		threshold = new Threshold(1000);
		trunkLock = new ReentrantReadWriteLock();
		loadLock = new ReentrantLock();
		reset();
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
		return completedTaskCount.get() > mergedTaskCount.get();
	}
	
	public boolean needExport()
	{
		return merged && mergedTaskCount.get() == taskCount;
	}

	public boolean needReset()
	{
		long consume = System.currentTimeMillis() - startTime;
				
		if ((exported && (consume >= jobConfig.getJobResetTime() * 1000))
				||(consume > jobConfig.getJobResetTime() * 1000 * 2))
			return true;
		
		if (mergedTaskCount.get() < taskCount && consume > jobConfig.getJobResetTime() * 1000)
			if (logger.isWarnEnabled() && threshold.sholdBlock())
				logger.warn("job : " + jobName + " can't complete in time!");
		
		return false;
	}
	
	public void reset()
	{
		for(JobTask task : jobTasks)
		{
			task.setStatus(JobTaskStatus.UNDO);
			task.setCreatTime(System.currentTimeMillis());
			task.getRecycleCounter().set(0);
		}
			
		taskCount = jobTasks.size();
		
		completedTaskCount = new AtomicInteger(0);
		mergedTaskCount = new AtomicInteger(0);
		needLoadResultFile = new AtomicBoolean(true);
		startTime = System.currentTimeMillis();
		merged = false;
		merging = false;
		exporting = false;
		exported = false;
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
	
	public boolean isMerged() {
		return merged;
	}


	public void setMerged(boolean merged) {
		this.merged = merged;
	}


	public boolean isMerging() {
		return merging;
	}


	public void setMerging(boolean merging) {
		this.merging = merging;
	}


	public boolean isExporting() {
		return exporting;
	}


	public void setExporting(boolean exporting) {
		this.exporting = exporting;
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


	public boolean isExported() {
		return exported;
	}

	public void setExported(boolean exported) {
		this.exported = exported;
	}


	public Map<String, Map<String, Object>> getDiskResult() {
		return diskResult;
	}


	public void setDiskResult(Map<String, Map<String, Object>> diskResult) {
		this.diskResult = diskResult;
	}
	

}
