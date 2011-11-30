/**
 * 
 */
package com.taobao.top.analysis.config;


/**
 * 服务端配置类
 * @author fangweng
 *
 */
public class MasterConfig extends AbstractConfig{
	
	static
	{
		processorCount = Runtime.getRuntime().availableProcessors();
	}

	private static int processorCount = 1;
	/**
	 * 
	 */
	private static final long serialVersionUID = 4127398444132837605L;
	
	private final static String MASTER_NAME = "masterName";
	
	private final static String MASTER_PORT = "masterPort";
	
	/**
	 * JobManager处理消息事件最大线程池线程数量
	 */
	private final static String MAX_JOBEVENT_WORKER = "maxJobEventWorker";
	
	/**
	 * 用于合并任务结果的线程最大设置，默认是当前处理器数-1
	 */
	private final static String MAX_MERGE_JOB_WORKER = "maxMergeJobWorker";
	
	/**
	 * 最大的用于输出统计分析结果或者载入导出临时结果的线程数，默认10
	 */
	private final static String MAX_CREATE_REPORT_WORKER = "maxCreateReportWorker";
	
	/**
	 * 任务配置的来源，可以自己扩展为DB，HTTP等方式，现在默认实现本地配置文件（file:xxxx）
	 */
	private final static String JOBS_SOURCE = "jobsSource";
	

	/**
	 * 合并最小的结果数，默认为1,如果不达到这个值，等待MAX_JOB_RESULT_BUNDLE_WAITTIME到获得必要个数为止
	 */
	private final static String MIN_MERGE_JOB_COUNT ="minMergeJobCount";

	/**
	 * 设置了minMergeJobCount,单位毫秒，最大等待组成一个bundle批量处理的时间，默认为60秒
	 */
	private final static String MAX_JOB_RESULT_BUNDLE_WAITTIME ="maxJobResultBundleWaitTime";

	/**
	 * 输出临时文件间隔(单位秒，默认10分钟),如果设置了saveTmpResultToFile，该时间设置无效，每次都会有导出临时文件
	 */
	private final static String EXPORT_INTERVAL = "exportInterval";
	
	// mod by fangweng 2011 performance
	//导出后清除对应的map内的数据，下次合并过程中再尝试从磁盘载入，通过配置开关判断是否实施
	private final static String SAVE_TMP_RESULT_TO_FILE = "saveTmpResultToFile";
	
	//mod by fangweng 2011 performance
	//配合磁盘换内存的方式，判断什么时候可以异步载入文件
	private final static String ASYN_LOAD_DISK_FILE_PRECENT = "asynLoadDiskFilePrecent";

	
	public String getMasterName() {
		if(this.properties.containsKey(MASTER_NAME))
			return (String)this.properties.get(MASTER_NAME);
		else
			return null;
	}

	public void setMasterName(String masterName) {
		this.properties.put(MASTER_NAME,masterName);
	}

	public int getMasterPort() {
		if(this.properties.containsKey(MASTER_PORT))
			return Integer.parseInt((String)this.properties.get(MASTER_PORT));
		else
			return 7777;
	}

	public void setMasterPort(String masterPort) {
		this.properties.put(MASTER_PORT,masterPort);
	}

	public String getJobsSource()
	{
		if(this.properties.containsKey(JOBS_SOURCE))
			return (String)this.properties.get(JOBS_SOURCE);
		else
			return null;
	}
	
	public void setJobsSource(String jobsSource)
	{
		this.properties.put(JOBS_SOURCE,jobsSource);
	}
	
	public int getMaxJobEventWorker() {
		if(this.properties.containsKey(MAX_JOBEVENT_WORKER))
			return Integer.parseInt((String)this.properties.get(MAX_JOBEVENT_WORKER));
		else
			return 50;
	}

	public void setMaxJobEventWorker(String maxJobEventWorker) {
		this.properties.put(MAX_JOBEVENT_WORKER, maxJobEventWorker);
	}
	
	
	public int getMaxMergeJobWorker() {
		if(this.properties.containsKey(MAX_MERGE_JOB_WORKER))
			return Integer.parseInt((String)this.properties.get(MAX_MERGE_JOB_WORKER));
		else
		{
			return  processorCount + 1;
		}
	}

	public void setMaxMergeJobWorker(String maxMergeJobWorker) {
		this.properties.put(MAX_MERGE_JOB_WORKER, maxMergeJobWorker);
	}

	public int getMaxCreateReportWorker() {
		if(this.properties.containsKey(MAX_CREATE_REPORT_WORKER))
			return Integer.parseInt((String)this.properties.get(MAX_CREATE_REPORT_WORKER));
		else
			return 10;
	}

	public void setMaxCreateReportWorker(String maxCreateReportWorker) {
		this.properties.put(MAX_CREATE_REPORT_WORKER, maxCreateReportWorker);
	}

	public int getMinMergeJobCount() {
		if(this.properties.containsKey(MIN_MERGE_JOB_COUNT))
			return Integer.parseInt((String)this.properties.get(MIN_MERGE_JOB_COUNT));
		else
			return 1;
	}

	public void setMinMergeJobCount(String minMergeJobCount) {
		this.properties.put(MIN_MERGE_JOB_COUNT, minMergeJobCount);
	}

	public long getMaxJobResultBundleWaitTime() {
		if(this.properties.containsKey(MAX_JOB_RESULT_BUNDLE_WAITTIME))
			return Long.parseLong((String)this.properties.get(MAX_JOB_RESULT_BUNDLE_WAITTIME)) * 1000;
		else
			return 60 * 1000;
	}

	public void setMaxJobResultBundleWaitTime(String maxJobResultBundleWaitTime) {
		this.properties.put(MAX_JOB_RESULT_BUNDLE_WAITTIME,maxJobResultBundleWaitTime);
	}

	public long getExportInterval() {
		if(this.properties.containsKey(EXPORT_INTERVAL))
			return Long.parseLong((String)this.properties.get(EXPORT_INTERVAL)) * 1000;
		else
			return 600 * 1000;
	}

	public void setExportInterval(String exportInterval) {
		this.properties.put(EXPORT_INTERVAL,exportInterval);
	}

	public boolean getSaveTmpResultToFile() {
		if(this.properties.containsKey(SAVE_TMP_RESULT_TO_FILE))
			return Boolean.parseBoolean((String)this.properties.get(SAVE_TMP_RESULT_TO_FILE));
		else
			return true;
	}

	public void setSaveTmpResultToFile(String saveTmpResultToFile) {
		this.properties.put(SAVE_TMP_RESULT_TO_FILE,saveTmpResultToFile);
	}

	public int getAsynLoadDiskFilePrecent() {
		if(this.properties.containsKey(ASYN_LOAD_DISK_FILE_PRECENT))
			return Integer.parseInt((String)this.properties.get(ASYN_LOAD_DISK_FILE_PRECENT));
		else
			return 85;
	}

	public void setAsynLoadDiskFilePrecent(String asynLoadDiskFilePrecent) {
		this.properties.put(ASYN_LOAD_DISK_FILE_PRECENT,asynLoadDiskFilePrecent);
	}

}
