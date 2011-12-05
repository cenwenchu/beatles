/**
 * 
 */
package com.taobao.top.analysis.config;


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
	
	public int getMaxClientEventWaitTime() {
		if(this.properties.containsKey(MAX_CLIENT_EVENT_WAIT_TIME))
			return Integer.parseInt((String)this.properties.get(MAX_CLIENT_EVENT_WAIT_TIME));
		else
			return 10;
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

	public int getGetJobInterval() {
		if(this.properties.containsKey(GET_JOB_INTERVAL))
			return Integer.parseInt((String)this.properties.get(GET_JOB_INTERVAL)) * 1000;
		else
			return 3000;
	}

	public void setGetJobInterval(String getJobInterval) {
		this.properties.put(GET_JOB_INTERVAL,getJobInterval);
	}

	public int getAnalysisWorkerNum() {
		if(this.properties.containsKey(ANALYSIS_WORKER_NUM))
			return Integer.parseInt((String)this.properties.get(ANALYSIS_WORKER_NUM));
		else
			return 10;
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

}
