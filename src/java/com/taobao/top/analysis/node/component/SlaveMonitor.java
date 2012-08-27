package com.taobao.top.analysis.node.component;

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.connect.ISlaveConnector;
import com.taobao.top.analysis.node.event.SendMonitorInfoEvent;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;
import com.taobao.top.analysis.node.monitor.IMonitor;
import com.taobao.top.analysis.node.monitor.MonitorUtil;
import com.taobao.top.analysis.node.monitor.SlaveMonitorInfo;
import com.taobao.top.analysis.util.NamedThreadFactory;

/**
 * Slave端口的监控器
 * @author sihai
 *
 */
public class SlaveMonitor implements IMonitor<SlaveConfig> {
	
	private static final Log logger = LogFactory.getLog(SlaveMonitor.class);
	
	/**
	 * Slave配置信息
	 */
	private SlaveConfig config;
	
	/**
	 * Slave监控信息
	 */
	private SlaveMonitorInfo monitorInfo;
	
	/**
	 * 通信层组件
	 */
	private ISlaveConnector slaveConnector;
	
	/**
	 * 定时汇报
	 */
	private ScheduledExecutorService executor;
	
	private long idGenerator = 0;
	
	@Override
	public SlaveConfig getConfig() {
		return config;
	}

	@Override
	public void init() throws AnalysisException {
		monitorInfo = new SlaveMonitorInfo();
		monitorInfo.setSlaveStartupTime(new Date());
		executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Slave-Monitor", true));
		executor.scheduleWithFixedDelay(new ReportTask(), config.getSlaveReportMonitorInterval() * 2, config.getSlaveReportMonitorInterval(), TimeUnit.SECONDS);
	}

	@Override
	public void releaseResource() {
		executor.shutdownNow();
	}

	@Override
	public void setConfig(SlaveConfig config) {
		this.config = config;
	}
	
	/**
	 * 统计一次任务拉取的情况
	 * @param time
	 */
	public void putPullTaskConsumeTime(long time, int taskCount) {
		monitorInfo.setSlavePullTaskConsumeTime(monitorInfo.getSlavePullTaskConsumeTime() + time);
		monitorInfo.setSlaveTryPullTaskCount(monitorInfo.getSlaveTryPullTaskCount() + 1);
		monitorInfo.setSlavePulledTaskCount(monitorInfo.getSlavePulledTaskCount() + taskCount);
		monitorInfo.setAveragePullTaskConsumeTime(monitorInfo.getSlavePullTaskConsumeTime() / (monitorInfo.getSlaveTryPullTaskCount() + 0.0D));
		monitorInfo.setAveragePulledTaskCount(monitorInfo.getSlavePulledTaskCount() / (monitorInfo.getSlaveTryPullTaskCount() + 0.0D));
	}
	
	/**
	 * 
	 * @param count
	 */
	public void incPulledTaskCount(long count) {
		// monitorInfo的slavePulledTaskCount声明为volatile能保证线程安全, 只有一个线程修改slavePulledTaskCount, 一个线程读取
		monitorInfo.setSlavePulledTaskCount(monitorInfo.getSlavePulledTaskCount() + count);
	}
	
	/**
	 * 任务执行统计
	 * @param count
	 */
	public void executedTask(long time, Collection<JobTaskExecuteInfo> infoList) {
		
		logger.warn(String.format("executedTask, time=%d, infoList.size=%d", time, infoList.size()));
		long dataSize = 0;
		long line = 0;
		long emptyLine = 0;
		long exceptionLine = 0;
		long keyCount = 0;
		long valueCount = 0;
		
		for(JobTaskExecuteInfo info : infoList) {
			dataSize += info.getJobDataSize();
			line += info.getTotalLine();
			emptyLine += info.getEmptyLine();
			exceptionLine += info.getErrorLine();
			keyCount += info.getKeyCount();
			valueCount += info.getValueCount();
		}
		
		// 增加执行时间
		monitorInfo.incSlaveExecuteTaskTime(time);
		// 增加完成的任务数量
		monitorInfo.incSlaveExecutedTaskCount(infoList.size());
		// 增加消耗的数据量
		monitorInfo.incSlaveConsumeDataSize(dataSize);
		// 增加消耗的数据行
		monitorInfo.incSlaveConsumeDataLine(line);
		// 增加消耗的空数据行
		monitorInfo.incSlaveConsumeEmptyLine(emptyLine);
		// 增加消耗的异常数据行
		monitorInfo.incSlaveConsumeExceptionLine(exceptionLine);
		// 增加产生的key数量
		monitorInfo.incKeyCount(keyCount);
		// 增加产生的value数量
		monitorInfo.incValueCount(valueCount);
	}
	
	/**
	 * 任务merge统计
	 * @param time
	 * @param taskCount
	 */
	public void mergedTask(long time, long taskCount) {
		monitorInfo.incMergedTaskCount(taskCount);
		monitorInfo.incMegeredTaskConsumeTime(time);
	}
	
	public void setSlaveConnector(ISlaveConnector slaveConnector) {
		this.slaveConnector = slaveConnector;
	}
	
	private class ReportTask implements Runnable {
		
		@Override
		public void run() {
			try {
				SendMonitorInfoEvent event = new SendMonitorInfoEvent("monitor-info-" + idGenerator++);
				event.setSlaveMonitorInfo((SlaveMonitorInfo)monitorInfo.clone());
				MonitorUtil.monitor(event.getSlaveMonitorInfo());
				slaveConnector.sendMonitorInfo(event);
			} catch (CloneNotSupportedException e) {
				logger.error("Exception:", e);
			}
		}
	}
}
