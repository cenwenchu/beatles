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
	
	private final static String MASTER_ADDRESS = "masterAddress";
	
	private final static String MASTER_PORT = "masterPort";
	
	/**
	 * 获取Job的间隔时间，当发现master没有任务的时候，用于slave休息，避免频繁获取Master的任务，单位毫秒,默认3秒
	 */
	private final static String GET_JOB_INTERVAL = "getJobInterval";
	
	/**
	 * 最大的客户端分析线程设置，一般每一个task给一个线程，支持一次获取多个task,默认20个线程
	 */
	private final static String ANALYSIS_WORKER_NUM = "analysisWorkerNum";
	
	/**
	 * 一次性最多获取多少个task，用slave来分担服务端合并压力，默认2个
	 */
	private final static String MAX_TRANSJOB_COUNT = "maxTransJobCount";
	
	

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

}
