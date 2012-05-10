/**
 * 
 */
package com.taobao.top.analysis.config;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.util.ReportUtil;


/**
 * Slave配置类
 * @author fangweng
 *
 */
public class SlaveConfig extends AbstractConfig {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2947907535268615055L;
	
	/**
	 * Slave的名称
	 */
	private final static String SLAVE_NAME = "slaveName";
	
	/**
	 * mater的地址
	 */
	private final static String MASTER_ADDRESS = "masterAddress";
	
	/**
	 * master端口
	 */
	private final static String MASTER_PORT = "masterPort";
	
	/**
	 * 是否指定只处理某一个Job的Task，默认不设置
	 */
	private final static String JOB_NAME = "jobName";
	
	/**
	 * 获取Job的间隔时间，当发现master没有任务的时候，用于slave休息，避免频繁获取Master的任务，单位毫秒,默认3秒
	 */
	private final static String GET_JOB_INTERVAL = "getJobInterval";
	
	/**
	 * 最大的客户端分析线程设置，一般每一个task给一个线程，支持一次获取多个task,默认10个线程
	 */
	private final static String ANALYSIS_WORKER_NUM = "analysisWorkerNum";
	
	/**
	 * 一次性最多获取多少个task，用slave来分担服务端合并压力，默认2个
	 */
	private final static String MAX_TRANSJOB_COUNT = "maxTransJobCount";
	
	/**
	 * 单个任务最大执行允许的时间,单位秒，默认5分钟
	 */
	private final static String MAX_TASK_PROCESS_TIME = "maxTaskProcessTime";
	
	/**
	 * 多个任务允许执行的最大时间,单位秒，默认10分钟 
	 */
	private final static String MAX_BUNDLE_PROCESS_TIME = "maxBundleProcessTime";
	
	/**
	 * 最大的客户端请求事件等待时间，就是客户端发起请求等待服务端返回事件响应的时间，单位秒，默认10秒
	 */
	private final static String MAX_CLIENT_EVENT_WAIT_TIME = "maxClientEventWaitTime";
	
	/**
	 * 当往多个master发送结果时没有成功的时候，将数据保存在本地
	 */
	private final static String TEMP_STORE_DATA_DIR = "tempStoreDataDir";
	
	/**
	 * 用于区分多个分析集群，一个分析集群可以由多个master和slave组成
	 */
	private final static String GROUP_ID = "groupId";
	
	/**
	 * 标识是否有zookeeper作为部分配置存储中心
	 */
	private final static String ZK_SERVER = "zkServer";
	
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
	
	public String getSlaveName()
	{
		if(this.properties.containsKey(SLAVE_NAME))
			return (String)this.properties.get(SLAVE_NAME);
		else
			return "_Default_Slave_" + ReportUtil.getIp();
	}
	
	public void setSlaveName(String slaveName)
	{
		this.properties.put(SLAVE_NAME,slaveName);
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
	
	public int getMaxClientEventWaitTime() {
		if(this.properties.containsKey(MAX_CLIENT_EVENT_WAIT_TIME))
			return Integer.parseInt((String)this.properties.get(MAX_CLIENT_EVENT_WAIT_TIME));
		else
			return 30;
	}

	public void setMaxClientEventWaitTime(String maxClientEventWaitTime) {
		this.properties.put(MAX_CLIENT_EVENT_WAIT_TIME,maxClientEventWaitTime);
	}

	public int getMaxTaskProcessTime() {
		if(this.properties.containsKey(MAX_TASK_PROCESS_TIME))
			return Integer.parseInt((String)this.properties.get(MAX_TASK_PROCESS_TIME));
		else
			return 5 * 60;
	}

	public void setMaxTaskProcessTime(String maxTaskProcessTime) {
		this.properties.put(MAX_TASK_PROCESS_TIME,maxTaskProcessTime);
	}
	
	public int getMaxBundleProcessTime() {
		if(this.properties.containsKey(MAX_BUNDLE_PROCESS_TIME))
			return Integer.parseInt((String)this.properties.get(MAX_BUNDLE_PROCESS_TIME));
		else
			return 10 * 60;
	}

	public void setMaxBundleProcessTime(String maxBundleProcessTime) {
		this.properties.put(MAX_BUNDLE_PROCESS_TIME,maxBundleProcessTime);
	}
	
	public String getMasterAddress() {
		if(this.properties.containsKey(MASTER_ADDRESS))
			return (String)this.properties.get(MASTER_ADDRESS);
		else
			return null;
	}

	public void setMasterAddress(String masterAddress) {
		this.properties.put(MASTER_ADDRESS,masterAddress);
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
	
	public String getJobName() {
		if(this.properties.containsKey(JOB_NAME))
			return (String)this.properties.get(JOB_NAME);
		else
			return null;
	}

	public void setJobName(String jobName) {
		this.properties.put(JOB_NAME,jobName);
	}

	public int getJobInterval() {
		if(this.properties.containsKey(GET_JOB_INTERVAL))
			return Integer.parseInt((String)this.properties.get(GET_JOB_INTERVAL)) * 1000;
		else
			return 3000;
	}

	public void setJobInterval(String getJobInterval) {
		this.properties.put(GET_JOB_INTERVAL,getJobInterval);
	}

	public int getAnalysisWorkerNum() {
		if(this.properties.containsKey(ANALYSIS_WORKER_NUM))
			return Integer.parseInt((String)this.properties.get(ANALYSIS_WORKER_NUM));
		else
			return 20;
	}

	public void setAnalysisWorkerNum(String analysisWorkerNum) {
		this.properties.put(ANALYSIS_WORKER_NUM,analysisWorkerNum);
	}

	public int getMaxTransJobCount() {
		if(this.properties.containsKey(MAX_TRANSJOB_COUNT))
			return Integer.parseInt((String)this.properties.get(MAX_TRANSJOB_COUNT));
		else
			return 2;
	}

	public void setMaxTransJobCount(String maxTransJobCount) {
		this.properties.put(MAX_TRANSJOB_COUNT,maxTransJobCount);
	}

	@Override
	public String marshal() {
		return new StringBuilder().append("timestamp=").append(System.currentTimeMillis()).append(",")
			    .append("groupId=").append(this.getGroupId()).append(",")
				.append("masterAddress=").append(this.getMasterAddress()).append(",")
				.append("masterPort=").append(this.getMasterPort()).append(",")
				.append("jobInterval=").append(this.getJobInterval()).append(",")
				.append("maxTransJobCount=").append(this.getMaxTransJobCount()).toString();
	}

	@Override
	public void unmarshal(String content) {
		if (StringUtils.isEmpty(content))
			return;
		
		String[] ct = content.split(",");
		
		if (ct.length >= 6)
		{
			this.setGroupId(ct[1].substring(ct[1].indexOf("=")+1));
			this.setMasterAddress(ct[2].substring(ct[2].indexOf("=")+1));
			this.setMasterPort(ct[3].substring(ct[3].indexOf("=")+1));
			this.setJobInterval(ct[4].substring(ct[4].indexOf("=")+1));
			this.setMaxTransJobCount(ct[5].substring(ct[5].indexOf("=")+1));
		}
	}

}
