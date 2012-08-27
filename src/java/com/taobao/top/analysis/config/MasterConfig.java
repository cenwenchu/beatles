/**
 * 
 */
package com.taobao.top.analysis.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.util.AnalyzerUtil;
import com.taobao.top.analysis.util.ReportUtil;


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

	/**
	 * 当前cpu数量
	 */
	private static int processorCount;
	/**
	 * 
	 */
	private static final long serialVersionUID = 4127398444132837605L;
	
	/**
	 * master的名称
	 */
	private final static String MASTER_NAME = "masterName";
	
	/**
	 * mater开启的端口
	 */
	private final static String MASTER_PORT = "masterPort";
	
	/**
	 * Master端输出系统监控信息目录
	 */
	private final static String SYSTEM_MONITOR_INFO_OUTPUT = "systemMonitorInfoOutput";
	
	/**
	 * JobManager处理消息事件最大线程池线程数量
	 */
	private final static String MAX_JOBEVENT_WORKER = "maxJobEventWorker";
	
	/**
	 * 用于合并任务结果的线程最大设置，默认是当前处理器数 + 3
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
	//数据量较小的情况下请勿使用！！！
	//导出后清除对应的map内的数据，下次合并过程中再尝试从磁盘载入，通过配置开关判断是否实施
	private final static String SAVE_TMP_RESULT_TO_FILE = "saveTmpResultToFile";
	
	//mod by fangweng 2011 performance
	//配合磁盘换内存的方式，判断什么时候可以异步载入文件
	private final static String ASYN_LOAD_DISK_FILE_PRECENT = "asynLoadDiskFilePrecent";
	
	//是否在JobMaster中采用异步模式去发送服务端的回执消息，比如在SocketConnector模式下就应该开启，memConnector模式下就不用
	//默认打开，根据自己实现和选择的Connector判断发送是否消耗来开关
	private final static String USE_ASYN_MODE_TO_SEND_RESPONSE = "useAsynModeToSendResponse";

	//支持多个master，不过其他master仅仅作为分担合并任务的工作，
	//主要目的就是分担主master的业务合并压力，部分报表可以定义给其他master合并
	//配置方式（name:ip:port|weight的方式,weight默认是1可以不填写）：masterGroup=TOPAnalyzer:127.0.0.1:6800,TOPAnalyzer1:127.0.0.1:6801
	private final static String MASTER_GROUP = "masterGroup";
	
	/**
	 * 当往多个master发送结果时没有成功的时候，将数据保存在本地
	 */
	private final static String TEMP_STORE_DATA_DIR = "tempStoreDataDir";
	
	/**
	 * 对于分析过程中的临时文件需要保存多久，用于做数据恢复的临时文件
	 */
	private final static String OLDDATA_KEEP_MINUTES = "oldDataKeepMinutes";
	
	/**
	 * 用于区分多个分析集群，一个分析集群可以由多个master和slave组成
	 */
	private final static String GROUP_ID = "groupId";
	
	/**
	 * 用于配置指定的报表分发到指定Master上面
	 */
	private final static String REPORT_TO_MASTER = "report2Master";
	
	/**
	 * 标识是否有zookeeper作为部分配置存储中心
	 */
	private final static String ZK_SERVER = "zkServer";
	
	/**
	 * 监控HTTP服务文档目录
	 */
	private final static String MONITOR_DOC_ROOT = "monitorDocRoot";
	
	private final static String DEFAULT_MONITOR_DOC_ROOT = "/tmp";
	
	/**
	 * 监控HTTP服务端口
	 */
	private final static String MONITOR_PORT = "monitorPort";
	
	/**
	 * Master导出监控新的周期, 到出到报表
	 */
	private final static String EXPORT_MONITOR_INTERVAL = "exportMonitorInterval";
	
	/**
	 * 是否为DispatchMaster
	 */
	private final static String DISPATCH_MASTER = "dispatchMaster";
	
	public String getZkServer()
	{
		if(this.properties.containsKey(ZK_SERVER))
			return this.properties.get(ZK_SERVER);
		else
			return null;
	}
	
	public void setZkServer(String zkServer)
	{
		this.properties.put(ZK_SERVER,zkServer);
	}
	
	public String getGroupId()
	{
		if(this.properties.containsKey(GROUP_ID))
			return this.properties.get(GROUP_ID);
		else
			return "_default_group_";
	}
	
	public void setGroupId(String GroupId)
	{
		this.properties.put(GROUP_ID,GroupId);
	}
	
	public int getOldDataKeepMinutes()
	{
		if(this.properties.containsKey(OLDDATA_KEEP_MINUTES))
			return Integer.valueOf(this.properties.get(OLDDATA_KEEP_MINUTES));
		else
			return 120;
	}
	
	public void setOldDataKeepMinutes(String oldDataKeepMinutes)
	{
		this.properties.put(OLDDATA_KEEP_MINUTES,oldDataKeepMinutes);
	}
	
	public String getTempStoreDataDir()
	{
		if(this.properties.containsKey(TEMP_STORE_DATA_DIR))
			return (String)this.properties.get(TEMP_STORE_DATA_DIR);
		else
			return "temp";
	}
	
	public void setTempStoreDataDir(String tempStoreDataDir)
	{
		this.properties.put(TEMP_STORE_DATA_DIR,tempStoreDataDir);
	}
	
	public String getMasterGroup()
	{
		if(this.properties.containsKey(MASTER_GROUP))
			return (String)this.properties.get(MASTER_GROUP);
		else
			return null;
	}
	
	public void setMasterGroup(String masterGroup) {
		this.properties.put(MASTER_GROUP,masterGroup);
	}
	
	public boolean isUseAsynModeToSendResponse() {
		if(this.properties.containsKey(USE_ASYN_MODE_TO_SEND_RESPONSE))
			return Boolean.valueOf((String)this.properties.get(USE_ASYN_MODE_TO_SEND_RESPONSE));
		else
			return true;
	}

	public void setUseAsynModeToSendResponse(String useAsynModeToSendResponse) {
		this.properties.put(USE_ASYN_MODE_TO_SEND_RESPONSE,useAsynModeToSendResponse);
	}

	public String getMasterName() {
		if(this.properties.containsKey(MASTER_NAME))
			return (String)this.properties.get(MASTER_NAME);
		else
			return "_Default_Master_" + ReportUtil.getIp();
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
			return  processorCount + 3;
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

	public void setReportToMaster(String report2Master) {
	    this.properties.put(REPORT_TO_MASTER, report2Master);
	}
	
	public Map<String, String> getReportToMaster() {
	    if(this.properties.containsKey(REPORT_TO_MASTER))
	        return AnalyzerUtil.convertStringToMap(this.properties.get(REPORT_TO_MASTER), ",", "|");
	    else
	        return new HashMap<String, String>();
	}
	
	public String getMonitorDocRoot() {
		if(this.properties.containsKey(MONITOR_DOC_ROOT))
			return (String)this.properties.get(MONITOR_DOC_ROOT);
		else
			return DEFAULT_MONITOR_DOC_ROOT;
	}
	
	public int getMonitorPort() {
		if(this.properties.containsKey(MONITOR_PORT))
			return Integer.parseInt((String)this.properties.get(MONITOR_PORT));
		else
			return 8421;
	}
	
	public void setMonitorPort(int port) {
		this.properties.put(MONITOR_PORT, String.valueOf(port));
	}
	
	public int getExportMonitorInterval() {
		if(this.properties.containsKey(EXPORT_MONITOR_INTERVAL)) {
			return Integer.parseInt((String)this.properties.get(EXPORT_MONITOR_INTERVAL));
		} else {
			// 默认5分钟
			return 300;
		}
	}
	
	public void setExportMonitorInterval(int interval) {
		this.properties.put(EXPORT_MONITOR_INTERVAL, String.valueOf(interval));
	}
	
	public String getSystemMonitorInfoOutput() {
		if(this.properties.containsKey(SYSTEM_MONITOR_INFO_OUTPUT)) {
			return (String)this.properties.get(SYSTEM_MONITOR_INFO_OUTPUT);
		} else {
			// 默认5分钟
			return "./system";
		}
	}
	
	public void setSystemMonitorInfoOutput(String output) {
		this.properties.put(SYSTEM_MONITOR_INFO_OUTPUT, output);
	}
	
	public boolean getDispatchMaster() {
	    if(this.properties.containsKey(DISPATCH_MASTER)) {
	        return Boolean.parseBoolean((String)this.properties.get(DISPATCH_MASTER));
	    }
	    //默认为true
	    return true;
	}
	
	public void setDispatchMaster(String dispatchMaster) {
	    this.properties.put(DISPATCH_MASTER, dispatchMaster);
	}

	@Override
	public String marshal() {
		return new StringBuilder().append("timestamp=").append(System.currentTimeMillis()).append(",")
			    .append("groupId=").append(this.getGroupId()).append(",")
				.append("masterName=").append(this.getMasterName()).append(",")
				.append("masterPort=").append(this.getMasterPort()).append(",")
				.append("jobsSource=").append(this.getJobsSource()).append(",")
				.append("masterGroup=").append(this.getMasterGroup()).toString();
	}

	@Override
	public void unmarshal(String content) {
		if (StringUtils.isEmpty(content))
			return;
		
		String[] ct = content.split(",");
		
		if (ct.length >= 6)
		{
			this.setGroupId(ct[1].substring(ct[1].indexOf("=")+1));
			this.setMasterName(ct[2].substring(ct[2].indexOf("=")+1));
			this.setMasterPort(ct[3].substring(ct[3].indexOf("=")+1));
			this.setJobsSource(ct[4].substring(ct[4].indexOf("=")+1));
			this.setMasterGroup(ct[5].substring(ct[5].indexOf("=")+1));
		}
	}
}
