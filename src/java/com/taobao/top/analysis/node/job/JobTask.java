/**
 * 
 */
package com.taobao.top.analysis.node.job;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import com.taobao.top.analysis.config.JobConfig;
import com.taobao.top.analysis.statistics.data.Rule;

/**
 * 
 * 任务的真实需要执行的实例，每个计算节点根据描述的输入获取数据，
 * 根据规则来分析数据，最后返回结果到输出
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public class JobTask implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4539861249196752195L;
	
	private String jobName;
	private String taskId;
	private Rule statisticsRule;
	private String input;
	private String output;
	/**
	 * 此次分析的过程中对于一条纪录的分隔符定义
	 */
	private String splitRegex;
	private String inputEncoding;
	private String outputEncoding;
	/**
	 * 任务回收时间，分发给某个slave以后多久可以被回收再次分配
	 */
	private int taskRecycleTime;
	/**
	 * 任务执行状态
	 */
	private JobTaskStatus status;
	
	/**
	 * 任务创建时间
	 */
	private long creatTime;
	/**
	 * 任务开始时间
	 */
	private long startTime;
	/**
	 * 任务结束时间
	 */
	private long endTime;
	
	/**
	 * 该job回收计数
	 */
	private AtomicInteger recycleCounter;
	
	/**
	 * 该task所属的job分配时的代数
	 */
	private int jobEpoch;
	
	/**
	 * 这个任务最后合并的年代纪录，为了当这台服务器出现问题的时候可以接受slave容灾时发送过来的老的结果
	 */
	private int lastMergedEpoch;
	
	/**
	 * 对于http请求
	 */
	private String url;
	
	/**
     * 用于系统恢复的时候从临时备份数据中读取临时数据在日志获取端的游标，以时间戳作为游标
     * 简单来说就是每一个master备份出去的临时文件包含当前分析的数据内容和这些数据对应到数据源（日志产生方）的日志拖拉绝对游标
     * 这个参数将会配合epoch一起加入到job的task的input中作为参数传递，epoch＝1的时候才会判断是否要根据这个参数重置游标
     */
    private long jobSourceTimeStamp = 0;
    
    /**
     * 重构状态，默认为0；
     * 若重构时需要修改该task，则置为1；
     */
    private int rebuildStatus = 0;
	
	public JobTask(JobConfig jobConfig)
	{
		input = jobConfig.getInput();
		output = jobConfig.getOutput();
		splitRegex = jobConfig.getSplitRegex();
		inputEncoding = jobConfig.getInputEncoding();
		outputEncoding = jobConfig.getOutputEncoding();
		taskRecycleTime = jobConfig.getTaskRecycleTime();
		status = JobTaskStatus.UNDO;
		creatTime = System.currentTimeMillis();
		recycleCounter= new AtomicInteger(0);
	}
	
	
	public int getLastMergedEpoch() {
		return lastMergedEpoch;
	}



	public void setLastMergedEpoch(int lastMergedEpoch) {
		this.lastMergedEpoch = lastMergedEpoch;
	}



	public int getJobEpoch() {
		return jobEpoch;
	}

	public void setJobEpoch(int jobEpoch) {
		this.jobEpoch = jobEpoch;
	}
	
	public String getJobName() {
		return jobName;
	}


	public void setJobName(String jobName) {
		this.jobName = jobName;
	}



	public String getOutputEncoding() {
		return outputEncoding;
	}

	public void setOutputEncoding(String outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	
	public JobTaskStatus getStatus() {
		return status;
	}
	public void setStatus(JobTaskStatus status) {
		this.status = status;
	}
	public long getCreatTime() {
		return creatTime;
	}
	public void setCreatTime(long creatTime) {
		this.creatTime = creatTime;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public AtomicInteger getRecycleCounter() {
		return recycleCounter;
	}
	public void setRecycleCounter(AtomicInteger recycleCounter) {
		this.recycleCounter = recycleCounter;
	}
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public Rule getStatisticsRule() {
		return statisticsRule;
	}
	public void setStatisticsRule(Rule statisticsRule) {
		this.statisticsRule = statisticsRule;
	}
	public String getInput() {
		return input;
	}
	public void setInput(String input) {
		this.input = input;
	}
	public String getOutput() {
		return output;
	}
	public void setOutput(String output) {
		this.output = output;
	}
	public String getSplitRegex() {
		return splitRegex;
	}
	public void setSplitRegex(String splitRegex) {
		this.splitRegex = splitRegex;
	}
	public String getInputEncoding() {
		return inputEncoding;
	}
	public void setInputEncoding(String inputEncoding) {
		this.inputEncoding = inputEncoding;
	}

	public int getTaskRecycleTime() {
		return taskRecycleTime;
	}

	public void setTaskRecycleTime(int taskRecycleTime) {
		this.taskRecycleTime = taskRecycleTime;
	}


    /**
     * @return the url
     */
    public String getUrl() {
        return url;
    }


    /**
     * @param url the url to set
     */
    public void setUrl(String url) {
        this.url = url;
    }
    
    public void resetInput() {
        if(url != null && url.startsWith("http")) {
            StringBuilder result = new StringBuilder(url);
            result.append("&jobSourceTimeStamp=").append(this.getJobSourceTimeStamp())
            .append("&epoch=").append(this.jobEpoch);
            this.input = result.toString();
        }
    }


    /**
     * @return the jobSourceTimeStamp
     */
    public long getJobSourceTimeStamp() {
        return jobSourceTimeStamp;
    }


    /**
     * @param jobSourceTimeStamp the jobSourceTimeStamp to set
     */
    public void setJobSourceTimeStamp(long jobSourceTimeStamp) {
        this.jobSourceTimeStamp = jobSourceTimeStamp;
    }
    
    /**
     * 
     * @param jobTask
     */
    public void rebuild(JobTask jobTask) {
        this.statisticsRule = jobTask.statisticsRule;
        this.inputEncoding = jobTask.inputEncoding;
        this.output = jobTask.output;
        this.outputEncoding = jobTask.outputEncoding;
        this.splitRegex = jobTask.splitRegex;
        this.taskRecycleTime = jobTask.taskRecycleTime;
        this.rebuildStatus = 1;
    }


    /**
     * @return the rebuildStatus
     */
    public int getRebuildStatus() {
        return rebuildStatus;
    }


    /**
     * @param rebuildStatus the rebuildStatus to set
     */
    public void setRebuildStatus(int rebuildStatus) {
        this.rebuildStatus = rebuildStatus;
    }

}
