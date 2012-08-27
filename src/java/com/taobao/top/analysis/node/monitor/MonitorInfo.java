package com.taobao.top.analysis.node.monitor;

import java.io.Serializable;

/**
 * 节点监控信息
 * @author sihai
 *
 */
public class MonitorInfo implements Serializable, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4354426032378192732L;
	
	// Title
	private static final String TITLE_TIMESTAMP = "时间戳";
	private static final String TITLE_IP = "IP地址";
	private static final String TITLE_OS_NAME = "操作系统";
	private static final String TITLE_OS_VERSION = "操作系统版本";
	private static final String TITLE_SYSTEM_LOAD_AVERAGE = "一分钟系统的负载";
	private static final String TITLE_JVM_TOTAL_MEMORY = "JVM持有的内存总大小";
	private static final String TITLE_JVM_FREE_MEMORY = "JVM空闲内存大小";
	private static final String TITLE_JVM_MAX_MEMORY = "JVM可使用的最大内存大小";
	private static final String TITLE_JVM_THREAD_COUNT = "JVM活动线程数";
	private static final String TITLE_JVM_PEAK_THREAD_COUNT = "JVM活动线程数峰值";
	
	/**
	 * 
	 */
	private long timeStamp;
	
	/**
	 * 机器IP
	 */
	private volatile String ip;
	
	// OS
	/**
	 * 操作系统
	 */
	private volatile String osName;
	
	/**
	 * 操作系统版本
	 */
	private volatile String osVersion;
	
	// 系统
	/**
	 * 过去一分钟系统的负载
	 */
	private volatile Double systemLoadAverage;
	
	// CPU
	
	// JVM Memory
	/**
	 * JVM持有的内存总大小, 单位byte
	 */
	private volatile Long jvmTotalMemory;
	
	/**
	 * JVM空闲内存大小, 单位byte
	 */
	private volatile Long jvmFreeMemory;
	
	/**
	 * JVM可使用的最大内存大小, 单位byte
	 * Note:如果没有限制的话, 返回java.lang.Long#MAX_VALUE, 当然会受到操作系统和物理机器的限制
	 */
	private volatile Long jvmMaxMemory;
	
	// JVM Thread
	/**
	 * 活着的总线程数, 包括daemon和non-daemon线程
	 */
	private volatile Integer jvmThreadCount;
	
	/**
	 * 自从 JVM启动或峰值重置以来峰值活动线程计数
	 */
	private volatile Integer jvmPeakThreadCount;
	
	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Long getJvmTotalMemory() {
		return jvmTotalMemory;
	}

	public void setJvmTotalMemory(Long jvmTotalMemory) {
		this.jvmTotalMemory = jvmTotalMemory;
	}

	public Long getJvmFreeMemory() {
		return jvmFreeMemory;
	}

	public void setJvmFreeMemory(Long jvmFreeMemory) {
		this.jvmFreeMemory = jvmFreeMemory;
	}

	public Long getJvmMaxMemory() {
		return jvmMaxMemory;
	}

	public void setJvmMaxMemory(Long jvmMaxMemory) {
		this.jvmMaxMemory = jvmMaxMemory;
	}

	public Integer getJvmThreadCount() {
		return jvmThreadCount;
	}

	public void setJvmThreadCount(Integer jvmThreadCount) {
		this.jvmThreadCount = jvmThreadCount;
	}

	public Integer getJvmPeakThreadCount() {
		return jvmPeakThreadCount;
	}

	public void setJvmPeakThreadCount(Integer jvmPeakThreadCount) {
		this.jvmPeakThreadCount = jvmPeakThreadCount;
	}

	public String getOsName() {
		return osName;
	}

	public void setOsName(String osName) {
		this.osName = osName;
	}

	public String getOsVersion() {
		return osVersion;
	}

	public void setOsVersion(String osVersion) {
		this.osVersion = osVersion;
	}

	public Double getSystemLoadAverage() {
		return systemLoadAverage;
	}

	public void setSystemLoadAverage(Double systemLoadAverage) {
		this.systemLoadAverage = systemLoadAverage;
	}
	
	public static String title() {
		StringBuilder sb = new StringBuilder(String.valueOf(TITLE_TIMESTAMP));
		sb.append(",");
		sb.append(TITLE_IP);
		sb.append(",");
		sb.append(TITLE_OS_NAME);
		sb.append(",");
		sb.append(TITLE_OS_VERSION);
		sb.append(",");
		sb.append(TITLE_SYSTEM_LOAD_AVERAGE);
		sb.append(",");
		sb.append(TITLE_JVM_TOTAL_MEMORY);
		sb.append(",");
		sb.append(TITLE_JVM_FREE_MEMORY);
		sb.append(",");
		sb.append(TITLE_JVM_MAX_MEMORY);
		sb.append(",");
		sb.append(TITLE_JVM_THREAD_COUNT);
		sb.append(",");
		sb.append(TITLE_JVM_PEAK_THREAD_COUNT);
		return sb.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(String.valueOf(timeStamp));
		sb.append(",");
		sb.append(ip);
		sb.append(",");
		sb.append(osName);
		sb.append(",");
		sb.append(osVersion);
		sb.append(",");
		sb.append(systemLoadAverage);
		sb.append(",");
		sb.append(jvmTotalMemory);
		sb.append(",");
		sb.append(jvmFreeMemory);
		sb.append(",");
		sb.append(jvmMaxMemory);
		sb.append(",");
		sb.append(jvmThreadCount);
		sb.append(",");
		sb.append(jvmPeakThreadCount);
		return sb.toString();
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		MonitorInfo clone = (MonitorInfo)super.clone();
		clone.setIp(ip);
		clone.setOsName(osName);
		clone.setOsVersion(osVersion);
		clone.setSystemLoadAverage(systemLoadAverage);
		clone.setJvmTotalMemory(jvmTotalMemory);
		clone.setJvmFreeMemory(jvmFreeMemory);
		clone.setJvmMaxMemory(jvmMaxMemory);
		clone.setJvmThreadCount(jvmThreadCount);
		clone.setJvmPeakThreadCount(jvmPeakThreadCount);
		return clone;
	}
}
