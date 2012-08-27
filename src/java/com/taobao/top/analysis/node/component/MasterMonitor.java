package com.taobao.top.analysis.node.component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.velocity.VelocityContext;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;
import com.taobao.top.analysis.node.monitor.IMonitor;
import com.taobao.top.analysis.node.monitor.JobExecutionLog;
import com.taobao.top.analysis.node.monitor.JobTaskExecutionLog;
import com.taobao.top.analysis.node.monitor.MasterMonitorInfo;
import com.taobao.top.analysis.node.monitor.SlaveMonitorInfo;
import com.taobao.top.analysis.util.ChartUtil;
import com.taobao.top.analysis.util.NamedThreadFactory;
import com.taobao.top.analysis.util.ChartUtil.LineEntry;

/**
 * Master端监控组件
 * @author sihai
 *
 */
public class MasterMonitor implements IMonitor<MasterConfig> {
	
	private static final String MONITOR_SYSTEM = "monitorSystem";					//
	private static final String MONITOR_JOB = "monitorJob";							//
	private static final String MONITOR_JOB_TASK = "monitorJobTask";				//
	private static final int MAX_CACHE_SIZE = 100;									//
	
	private static final Log logger = LogFactory.getLog(MasterMonitor.class);		//
	private static final Log systemLogger = LogFactory.getLog(MONITOR_SYSTEM);		//
	private static final Log jobLogger = LogFactory.getLog(MONITOR_JOB);			// 
	private static final Log jobTaskLogger = LogFactory.getLog(MONITOR_JOB_TASK);	//

	// 系统级别监控信息
	private MasterConfig config;										// Master端配置信息
	private ConcurrentHashMap<String, SlaveMonitorInfoContainer> cache;	// 缓存的最近的数据, 以为实时分析
	private ScheduledExecutorService executor;							// 导出报表线程, 单线程
	
	// job级别监控
	private ConcurrentHashMap<String, JobExecutionLogContainer> jobExecutionLogCache;	// 缓存的最近的数据, 以为实时分析
	
	@Override
	public MasterConfig getConfig() {
		return config;
	}

	@Override
	public void init() throws AnalysisException {
		cache = new ConcurrentHashMap<String, SlaveMonitorInfoContainer>();
		jobExecutionLogCache = new ConcurrentHashMap<String, JobExecutionLogContainer>();
		executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Master-Monitor", true));
		executor.scheduleWithFixedDelay(new ExportTask(), config.getExportMonitorInterval() * 2, config.getExportMonitorInterval(), TimeUnit.SECONDS);
		logger.info("monitor init end");
	}

	@Override
	public void releaseResource() {
		if(executor != null) {
			executor.shutdown();
		}
		if(cache != null) {
			cache.clear();
		}
		if(jobExecutionLogCache != null) {
			jobExecutionLogCache.clear();
		}
	}

	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}
	
	/**
	 * 接收Slave的监控信息, 只有一个线程会调用
	 * @param info
	 */
	public MasterMonitorInfo report(SlaveMonitorInfo info) {
		// 设置时间戳以Master为准
		info.setTimeStamp(System.currentTimeMillis());
		// 更新最新缓存数据
		updateCache(info);
		// 记录日志
		systemLogger.info(info);
		// 目前还没用, 暂且返回一个空的
		return new MasterMonitorInfo();
	}
	
	/**
	 * 接收Slave报告的任务执行统计信息
	 * @param infos
	 */
	public void report(String jobName, Collection<JobTaskExecuteInfo> infos) {
		
		if(infos.isEmpty()) {
			return;
		}
		
		// 更新最新缓存数据
		updateCache(jobName, infos);	
	}
	
	/**
	 * 
	 * @param context
	 */
	public void getData(VelocityContext context) {
		
		Map<String, List<SlaveMonitorInfo>> snapshot = new HashMap<String, List<SlaveMonitorInfo>>();
			
		// 构建每一个Slave最新的状态
		// 每一个Slave的当前快照信息
		List<SlaveMonitorInfo> slaveList = new ArrayList<SlaveMonitorInfo>(cache.size());
		SlaveMonitorInfoContainer container = null;
		for(Map.Entry<String, SlaveMonitorInfoContainer> entry : cache.entrySet()) {
			container = entry.getValue();
			try {
				container.lock.lock();
				snapshot.put(entry.getKey(), new ArrayList<SlaveMonitorInfo>(container.infoList));
				slaveList.add(container.infoList.get(container.infoList.size() - 1));
			} finally {
				container.lock.unlock();
			}
		}
		context.put("slaveList", slaveList);
		context.put("picList", draw(snapshot));
	}
	
	/**
	 * 更新缓存数据, 使每个slave的数据保持在最新的100个
	 * @param info
	 */
	private void updateCache(SlaveMonitorInfo info) {
		SlaveMonitorInfoContainer old = null;
		SlaveMonitorInfoContainer container = cache.get(info.getIp());
		if(container == null) {
			container = new SlaveMonitorInfoContainer();
			old = cache.putIfAbsent(info.getIp(), container);
			if(old != null) {
				container = old;
			}
		}
		try {
			container.lock.lock();
			if(container.infoList.size() == MAX_CACHE_SIZE) {
				// 移除最老的一个
				container.infoList.remove(0);
			}
			container.infoList.add(info);
		} finally {
			container.lock.unlock();
		}
	}
	
	private void updateCache(String jobName, Collection<JobTaskExecuteInfo> infos) {
		JobExecutionLogContainer old = null;
		JobExecutionLogContainer container = null;
		container = jobExecutionLogCache.get(jobName);
		if(container == null) {
			container = new JobExecutionLogContainer(jobName);
			old = jobExecutionLogCache.putIfAbsent(jobName, container);
			if(old != null) {
				container = old;
			}
		}
		try {
			container.lock.lock();
			JobTaskExecutionLog taskLog = null;
			for(JobTaskExecuteInfo info : infos) {
				taskLog = new JobTaskExecutionLog(jobName, info);
				if(container.logList.size() == MAX_CACHE_SIZE) {
					// 移除最老的一个
					container.logList.remove(0);
				}
				jobTaskLogger.info(taskLog);
				container.logList.add(taskLog);
				container.jobExecutionLog.plus(taskLog);
			}
		} finally {
			container.lock.unlock();
		}
	}
	
	private List<Pic> draw(Map<String, List<SlaveMonitorInfo>> snapshot) {
		
		String name = null;
		String fileName = null;
		List<Pic> picList = new ArrayList<Pic>();
		
		// 绘制各个Slave的最近一分钟load走势
		name = "最近一分钟load走势";
		fileName = "load.jpg";
		ChartUtil.drawLine(name, "时间", "Load", config
				.getMonitorDocRoot()
				+ File.separator + "images" + File.separator + fileName,
				getMultiLineData(snapshot, 0));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "JVM持有的内存总大小走势";
		fileName = "jvmTotalMemory.jpg";
		ChartUtil.drawLine(name, "时间", "JVM Total Memory", config
				.getMonitorDocRoot()
				+ File.separator
				+ "images"
				+ File.separator
				+ fileName, getMultiLineData(snapshot, 1));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "JVM空闲内存大小走势";
		fileName = "jvmFreeMemory.jpg";
		ChartUtil.drawLine(name, "时间", "JVM Free Memory", config
				.getMonitorDocRoot()
				+ File.separator
				+ "images"
				+ File.separator
				+ fileName, getMultiLineData(snapshot, 2));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "JVM可使用的最大内存大小走势";
		fileName = "jvmMaxMemory.jpg";
		ChartUtil.drawLine(name, "时间", "JVM Max Memory", config
				.getMonitorDocRoot()
				+ File.separator
				+ "images"
				+ File.separator
				+ fileName, getMultiLineData(snapshot, 3));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "JVM活着的总线程数走势";
		fileName = "jvmThreadCount.jpg";
		ChartUtil.drawLine(name, "时间", "JVM Live Thread Count",
				config.getMonitorDocRoot() + File.separator + "images"
						+ File.separator + fileName,
				getMultiLineData(snapshot, 4));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "自从 JVM启动或峰值重置以来峰值活动线程计数走势";
		fileName = "jvmPeakThreadCount.jpg";
		ChartUtil.drawLine(name, "时间",
				"JVM Peak Thread Count", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						5));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "Map key总数走势";
		fileName = "mapKey.jpg";
		ChartUtil.drawLine(name, "时间",
				"Map Key", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						9));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "Map value总数走势";
		fileName = "mapValue.jpg";
		ChartUtil.drawLine(name, "时间",
				"Map Value", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						10));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "处理数据总大小走势";
		fileName = "consumeDataSize.jpg";
		ChartUtil.drawLine(name, "时间",
				"Consume Data Size", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						11));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "处理数据总行数走势";
		fileName = "consumeDataLine.jpg";
		ChartUtil.drawLine(name, "时间",
				"Consume Data Line", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						12));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "处理空数据行数走势";
		fileName = "consumeEmptyDataLine.jpg";
		ChartUtil.drawLine(name, "时间",
				"Consume Empty Data Line", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						13));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "处理异常数据行数走势";
		fileName = "consumeExceptionDataLine.jpg";
		ChartUtil.drawLine(name, "时间",
				"Consume Exception Data Line", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						14));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "Slave尝试拉取任务次数趋势";
		fileName = "slaveTryPullTaskCount.jpg";
		ChartUtil.drawLine(name, "时间",
				"Slave Try Pull Task Count", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						19));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "Slave消耗在拉取任务的时间";
		fileName = "slavePullTaskConsumeTime.jpg";
		ChartUtil.drawLine(name, "时间",
				"Slave Pull Task Consume Time", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						18));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "Slave拉取任务总数";
		fileName = "slavePulledTaskCount.jpg";
		ChartUtil.drawLine(name, "时间",
				"Slave Pulled Task Count", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						17));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "Slave消耗在执行任务的时间";
		fileName = "slaveExecuteTaskTime.jpg";
		ChartUtil.drawLine(name, "时间",
				"Slave Execute Task Consume Time", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						16));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "Slave执行任务总数";
		fileName = "slaveExecutedTaskCount.jpg";
		ChartUtil.drawLine(name, "时间",
				"Slave Executed Task Count", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						15));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "一个任务平均消耗时间趋势";
		fileName = "averageTaskConsumeTime.jpg";
		ChartUtil.drawLine(name, "时间",
				"Average Task Consume Time", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						8));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "平均每次成功拉取任务个数趋势";
		fileName = "averagePulledTaskCount.jpg";
		ChartUtil.drawLine(name, "时间",
				"Average Pulled Task Count", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						6));
		picList.add(new Pic(name, "/images/" + fileName));
		
		name = "平均每次拉取任务消耗时间趋势";
		fileName = "averagePullTaskConsumeTime.jpg";
		ChartUtil.drawLine(name, "时间",
				"Average Pull Task Consume Time", config.getMonitorDocRoot()
						+ File.separator + "images" + File.separator
						+ fileName, getMultiLineData(snapshot,
						7));
		picList.add(new Pic(name, "/images/" + fileName));
		return picList;
	}
	
	/**
	 * 
	 * @param snapshot
	 * @param type		
	 * @return
	 */
	private Map<String, List<LineEntry>> getMultiLineData(Map<String, List<SlaveMonitorInfo>> snapshot, int type) {
		Map<String, List<LineEntry>> data = new HashMap<String, List<LineEntry>>();
		for(Map.Entry<String, List<SlaveMonitorInfo>> entry : snapshot.entrySet()) {
			List<LineEntry> line = new ArrayList<LineEntry>();
			int time = 0;
			for(SlaveMonitorInfo info : entry.getValue()) {
				if(type == 0) {
					line.add(new LineEntry(time++, info.getSystemLoadAverage()));
				} else if(type == 1) {
					line.add(new LineEntry(time++, info.getJvmTotalMemory()));
				} else if(type == 2) {
					line.add(new LineEntry(time++, info.getJvmFreeMemory()));
				} else if(type == 3) {
					line.add(new LineEntry(time++, info.getJvmMaxMemory()));
				} else if(type == 4) {
					line.add(new LineEntry(time++, info.getJvmThreadCount()));
				} else if(type == 5) {
					line.add(new LineEntry(time++, info.getJvmPeakThreadCount()));
				} else if(type == 6) {
					line.add(new LineEntry(time++, info.getAveragePulledTaskCount()));
				} else if(type == 7) {
					line.add(new LineEntry(time++, info.getAveragePullTaskConsumeTime()));
				} else if(type == 8) {
					line.add(new LineEntry(time++, info.getAverageTaskConsumeTime()));
				} else if(type == 9) {
					line.add(new LineEntry(time++, info.getKeyCount()));
				} else if(type == 10) {
					line.add(new LineEntry(time++, info.getValueCount()));
				} else if(type == 11) {
					line.add(new LineEntry(time++, info.getSlaveConsumeDataSize()));
				} else if(type == 12) {
					line.add(new LineEntry(time++, info.getSlaveConsumeDataLine()));
				} else if(type == 13) {
					line.add(new LineEntry(time++, info.getSlaveConsumeEmptyLine()));
				} else if(type == 14) {
					line.add(new LineEntry(time++, info.getSlaveConsumeExceptionLine()));
				} else if(type == 15) {
					line.add(new LineEntry(time++, info.getSlaveExecutedTaskCount()));
				} else if(type == 16) {
					line.add(new LineEntry(time++, info.getSlaveExecuteTaskTime()));
				} else if(type == 17) {
					line.add(new LineEntry(time++, info.getSlavePulledTaskCount()));
				} else if(type == 18) {
					line.add(new LineEntry(time++, info.getSlavePullTaskConsumeTime()));
				} else if(type == 19) {
					line.add(new LineEntry(time++, info.getSlaveTryPullTaskCount()));
				}
			}
			data.put(entry.getKey(), line);
		}
		return data;
	}
	
	/**
	 * 
	 * @return
	 */
	private String generteOutputDirectoryName(long timestamp, String type) {
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		String currentTime = new StringBuilder()
			.append(calendar.get(Calendar.YEAR)).append("-")
			.append(calendar.get(Calendar.MONTH) + 1).append("-")
			.append(calendar.get(Calendar.DAY_OF_MONTH)).toString();
		StringBuilder sb = new StringBuilder();
		sb.append(config.getSystemMonitorInfoOutput())
			.append(config.getSystemName())
			.append(File.separator).append("period")
			.append(File.separator).append(currentTime)
			.append(File.separator).append(type)
			.append(File.separator);
		return sb.toString();
	}
	
	
	/**
	 * 导出Master的监控信息到报表
	 */
	private class ExportTask implements Runnable {

		@Override
		public void run() {
			// 导出系统级别监控信息到period报表
			exportSystemMonitorInfo();
			// 导出Job级别监控信息到period报表
			exportJobMonitorInfo();
			// 导出JobTask级别监控信息到period报表
			exportJobTaskMonitorInfo();
		}
	}
	
	/**
	 * 导出系统级别的监控信息
	 */
	private void exportSystemMonitorInfo() {
		long timestamp = System.currentTimeMillis();
		File dir = new File(generteOutputDirectoryName(timestamp, "system"));
		if (!dir.exists()) {
			dir.mkdirs();
		}
		String fileName = String.format("%s%s%d%s", dir.getAbsolutePath(), File.separator, timestamp, ".csv");
		BufferedWriter writer = null;
		
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"));
			// 写头
			writer.write(SlaveMonitorInfo.title());
			// 写内容
			String key = null;
			SlaveMonitorInfoContainer container = null;
			for(Iterator<String> it = cache.keySet().iterator(); it.hasNext();) {
				key = it.next();
				container = cache.get(key);
				if(container != null) {
					try {
						container.lock.lock();
						for(SlaveMonitorInfo info : container.infoList) {
							writer.newLine();
							writer.write(info.toString());
						}
						container.infoList.clear();
					} finally {
						container.lock.unlock();
					}
					
				}
				cache.remove(key);
			}
			writer.flush();
		} catch (IOException e) {
			logger.error("Export monitor info failed:", e);
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.error("Export monitor info failed:", e);
				}
			}
		}
	}
	
	/**
	 * 导出Job级别的监控信息
	 */
	private void exportJobMonitorInfo() {
		long timestamp = System.currentTimeMillis();
		File dir = new File(generteOutputDirectoryName(timestamp, "job"));
		if (!dir.exists()) {
			dir.mkdirs();
		}
		String fileName = String.format("%s%s%d%s", dir.getAbsolutePath(), File.separator, timestamp, ".csv");
		dir = new File(generteOutputDirectoryName(timestamp, "jobTask"));
		if (!dir.exists()) {
			dir.mkdirs();
		}
		String fileName2 = String.format("%s%s%d%s", dir.getAbsolutePath(), File.separator, timestamp, ".csv");
		
		BufferedWriter writer = null;
		BufferedWriter writer2 = null;
		
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"));
			writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName2), "utf-8"));
			// 写头
			writer.write(JobExecutionLog.title());
			writer2.write(JobTaskExecutionLog.title());
			// 写内容
			String key = null;
			JobExecutionLogContainer container = null;
			for(Iterator<String> it = jobExecutionLogCache.keySet().iterator(); it.hasNext();) {
				key = it.next();
				container = jobExecutionLogCache.get(key);
				if(container != null) {
					try {
						container.lock.lock();
						writer.newLine();
						writer.write(container.jobExecutionLog.toString());
						for(JobTaskExecutionLog info : container.logList) {
							writer2.newLine();
							writer2.write(info.toString());
						}
						container.logList.clear();
					} finally {
						container.lock.unlock();
					}
					
				}
				jobExecutionLogCache.remove(key);
			}
			writer.flush();
			writer2.flush();
		} catch (IOException e) {
			logger.error("Export monitor info failed:", e);
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.error("Export monitor info failed:", e);
				}
			}
			if(writer2 != null) {
				try {
					writer2.close();
				} catch (IOException e) {
					logger.error("Export monitor info failed:", e);
				}
			}
		}
	}
	
	private void exportJobTaskMonitorInfo() {
		
	}
	
	private class SlaveMonitorInfoContainer {
		public List<SlaveMonitorInfo> infoList = new ArrayList<SlaveMonitorInfo>();
		public ReentrantLock lock = new ReentrantLock();
	}
	
	/**
	 * 
	 */
	private class JobExecutionLogContainer {
		
		public JobExecutionLog jobExecutionLog;
		public List<JobTaskExecutionLog> logList;
		public ReentrantLock lock;
		
		public JobExecutionLogContainer(String jobName) {
			jobExecutionLog = new JobExecutionLog(jobName);
			logList = new ArrayList<JobTaskExecutionLog>();
			lock = new ReentrantLock();
		}
	}
	
	/**
	 *
	 *
	 */
	public static class Pic {
		
		private String name;
		private String url;
		
		public Pic(String name, String url) {
			this.name = name;
			this.url = url;
		}
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
	}
}
