package com.taobao.top.analysis.node.monitor;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;

import com.taobao.top.analysis.util.ReportUtil;


/**
 * 
 * @author sihai
 *
 */
public abstract class MonitorUtil {
	
	
	/**
	 * 收集系统, JVM信息
	 * @return
	 */
	public static void monitor(MonitorInfo info) {
		info.setIp(ReportUtil.getIp());
		info.setJvmTotalMemory(Runtime.getRuntime().totalMemory());
		info.setJvmFreeMemory(Runtime.getRuntime().freeMemory());
		info.setJvmMaxMemory(Runtime.getRuntime().maxMemory());
		
		OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		info.setOsName(osmxb.getName());
		info.setOsVersion(osmxb.getVersion());
		info.setSystemLoadAverage(osmxb.getSystemLoadAverage());
		
		ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
		info.setJvmThreadCount(tmxb.getThreadCount());
		info.setJvmPeakThreadCount(tmxb.getPeakThreadCount());
	}
}
