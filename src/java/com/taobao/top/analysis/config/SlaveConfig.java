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
	
	private String masterAddress;
	
	private String masterPort;
	
	private String getJobInterval;
	
	private String splitWorkerNum;
	
	private String analysisWorkerNum;
	
	private String maxTransJobCount;



}
