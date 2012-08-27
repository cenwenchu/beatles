package com.taobao.top.analysis.node.monitor;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Slave节点监控信息
 * @author sihai
 *
 */
public class SlaveMonitorInfo extends MonitorInfo implements Cloneable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7236590241815578921L;
	
	private static final String TITLE_SLAVE_STARTUP_TIME = "Slave启动时间";
	private static final String TITLE_SLAVE_TRY_PULL_TASK_COUNT = "Slave尝试拉取任务次数";
	private static final String TITLE_SLAVE_PULL_TASK_CONSUME_TIME = "Slave拉取任务花费的时间";
	private static final String TITLE_SLAVE_PULLED_TASK_COUNT = "Slave成功拉取的任务总数";
	private static final String TITLE_SLAVE_AVERAGE_PULL_TASK_CONSUME_TIME = "Slave平均每次拉取任务消耗时间";
	private static final String TITLE_SLAVE_AVERAGE_PULLED_TASK_COUNT = "平均每次成功拉取任务个数";
	private static final String TITLE_SLAVE_AVERAGE_TASK_CONSUME_TIME = "平均每个任务消耗时间";
	private static final String TITLE_SLAVE_EXECUTE_TASK_CONSUME_TIME = "Slave花费在执行任务的时间";
	private static final String TITLE_SLAVE_EXECUTEED_TASK_COUNT = "Slave成功执行的任务总数";
	private static final String TITLE_SLAVE_CONSUME_DATA_SIZE = "Slave处理数据总大小";
	private static final String TITLE_SLAVE_CONSUME_DATA_LINE = "Slave处理数据行总数";
	private static final String TITLE_SLAVE_CONSUME_EMPTY_LINE = "Slave处理空数据行总数";
	private static final String TITLE_SLAVE_CONSUME_EXCEPTION_LINE = "Slave处理异常数据行总数";
	private static final String TITLE_SLAVE_KEY_COUNT = "Slave Map Key总数";
	private static final String TITLE_SLAVE_VALUE_COUNT = "Slave Map Value总数";
	private static final String TITLE_SLAVE_MERGED_TASK_COUNT = "Slave Merged任务总数";
	private static final String TITLE_SLAVE_MERGED_TASK_CONSUME_TIME = "Slave 在Merged消耗的时间";
	
	// 
	/**
	 * Slave启动时间
	 */
	private Date slaveStartupTime;
	
	/**
	 * Slave尝试拉取任务次数, 不一定是每次都成功的或是每次都拉取到了任务
	 */
	private volatile long slaveTryPullTaskCount;
	
	/**
	 * Slave在拉取任务花费的时间, 单位毫秒
	 */
	private volatile long slavePullTaskConsumeTime;
	
	/**
	 * Slave成功拉取的任务总数
	 */
	private volatile long slavePulledTaskCount;
	
	
	// 任务
	/**
	 * Slave平均每次拉取任务消耗时间, 单位毫秒
	 */
	private volatile double averagePullTaskConsumeTime;
	
	/**
	 * 平均每次成功拉取任务个数
	 */
	private volatile double averagePulledTaskCount;
	
	/**
	 * 平均每个任务消耗时间
	 */
	private volatile long averageTaskConsumeTime;
	
	/**
	 * Slave花费在执行任务的时间, 单位毫秒
	 */
	private AtomicLong slaveExecuteTaskTime;

	/**
	 * Slave成功执行的任务总数
	 */
	private AtomicLong slaveExecutedTaskCount;
	
	
	// 数据
	/**
	 * Slave处理数据总大小
	 */
	private AtomicLong slaveConsumeDataSize;
	
	/**
	 * Slave处理数据行总数
	 */
	private AtomicLong slaveConsumeDataLine;
	
	/**
	 * Slave处理空数据行总数
	 */
	private AtomicLong slaveConsumeEmptyLine;
	
	/**
	 * Slave处理异常数据行总数
	 */
	private AtomicLong slaveConsumeExceptionLine;
	
	/**
	 * Slave Map key总数
	 */
	private AtomicLong keyCount;
	
	/**
	 * Slave Map value总数
	 */
	private AtomicLong valueCount;
	
	// merge
	/**
	 * merge多少task
	 */
	private AtomicLong mergedTaskCount;
	
	/**
	 * merge消耗多少时间, 单位毫秒
	 */
	private AtomicLong megeredTaskConsumeTime;
	
	
	public SlaveMonitorInfo() {
		slaveExecuteTaskTime = new AtomicLong(0L);
		slaveExecutedTaskCount = new AtomicLong(0L);
		slaveConsumeDataSize = new AtomicLong(0L);
		slaveConsumeDataLine = new AtomicLong(0L);
		slaveConsumeEmptyLine = new AtomicLong(0L);
		slaveConsumeExceptionLine = new AtomicLong(0L);
		keyCount = new AtomicLong(0L);
		valueCount = new AtomicLong(0L);
		mergedTaskCount = new AtomicLong(0L);
		megeredTaskConsumeTime = new AtomicLong(0L);
	}

	public Date getSlaveStartupTime() {
		return slaveStartupTime;
	}

	public void setSlaveStartupTime(Date slaveStartupTime) {
		this.slaveStartupTime = slaveStartupTime;
	}

	public long getSlaveTryPullTaskCount() {
		return slaveTryPullTaskCount;
	}

	public void setSlaveTryPullTaskCount(long slaveTryPullTaskCount) {
		this.slaveTryPullTaskCount = slaveTryPullTaskCount;
	}

	public long getSlavePullTaskConsumeTime() {
		return slavePullTaskConsumeTime;
	}

	public void setSlavePullTaskConsumeTime(long slavePullTaskConsumeTime) {
		this.slavePullTaskConsumeTime = slavePullTaskConsumeTime;
	}
	
	public double getAveragePullTaskConsumeTime() {
		return averagePullTaskConsumeTime;
	}

	public void setAveragePullTaskConsumeTime(double averagePullTaskConsumeTime) {
		this.averagePullTaskConsumeTime = averagePullTaskConsumeTime;
	}

	public double getAveragePulledTaskCount() {
		return averagePulledTaskCount;
	}

	public void setAveragePulledTaskCount(double averagePulledTaskCount) {
		this.averagePulledTaskCount = averagePulledTaskCount;
	}

	public long getAverageTaskConsumeTime() {
		return averageTaskConsumeTime;
	}

	public void setAverageTaskConsumeTime(long averageTaskConsumeTime) {
		this.averageTaskConsumeTime = averageTaskConsumeTime;
	}

	public long getSlaveExecuteTaskTime() {
		return slaveExecuteTaskTime.get();
	}

	public long incSlaveExecuteTaskTime(long slaveExecuteTaskTime) {
		return this.slaveExecuteTaskTime.addAndGet(slaveExecuteTaskTime);
	}
	
	public long getSlavePulledTaskCount() {
		return slavePulledTaskCount;
	}

	public void setSlavePulledTaskCount(long slavePulledTaskCount) {
		this.slavePulledTaskCount = slavePulledTaskCount;
	}

	public long getSlaveExecutedTaskCount() {
		return slaveExecutedTaskCount.get();
	}

	public long incSlaveExecutedTaskCount(long slaveExecutedTaskCount) {
		return this.slaveExecutedTaskCount.addAndGet(slaveExecutedTaskCount);
	}

	public long getSlaveConsumeDataSize() {
		return slaveConsumeDataSize.get();
	}

	public void incSlaveConsumeDataSize(long slaveConsumeDataSize) {
		this.slaveConsumeDataSize.addAndGet(slaveConsumeDataSize);
	}

	public long getSlaveConsumeDataLine() {
		return slaveConsumeDataLine.get();
	}

	public long incSlaveConsumeDataLine(long slaveConsumeDataLine) {
		return this.slaveConsumeDataLine.addAndGet(slaveConsumeDataLine);
	}

	public long getSlaveConsumeEmptyLine() {
		return slaveConsumeEmptyLine.get();
	}

	public void incSlaveConsumeEmptyLine(long slaveConsumeEmptyLine) {
		this.slaveConsumeEmptyLine.addAndGet(slaveConsumeEmptyLine);
	}

	public long getSlaveConsumeExceptionLine() {
		return slaveConsumeExceptionLine.get();
	}

	public long incSlaveConsumeExceptionLine(long slaveConsumeExceptionLine) {
		return this.slaveConsumeExceptionLine.addAndGet(slaveConsumeExceptionLine);
	}

	public long getKeyCount() {
		return keyCount.get();
	}

	public void incKeyCount(long keyCount) {
		this.keyCount.addAndGet(keyCount);
	}

	public long getValueCount() {
		return valueCount.get();
	}

	public void incValueCount(long valueCount) {
		this.valueCount.addAndGet(valueCount);
	}
	
	public long getMergedTaskCount() {
		return mergedTaskCount.get();
	}

	public void incMergedTaskCount(long mergedTaskCount) {
		this.mergedTaskCount.addAndGet(mergedTaskCount);
	}
	
	public long getMegeredTaskConsumeTime() {
		return megeredTaskConsumeTime.get();
	}

	public void incMegeredTaskConsumeTime(long megeredTaskConsumeTime) {
		this.megeredTaskConsumeTime.addAndGet(megeredTaskConsumeTime);
	}
	
	public static String title() {
		StringBuilder sb = new StringBuilder(MonitorInfo.title());
		sb.append(",");
		sb.append(TITLE_SLAVE_STARTUP_TIME);
		sb.append(",");
		sb.append(TITLE_SLAVE_TRY_PULL_TASK_COUNT);
		sb.append(",");
		sb.append(TITLE_SLAVE_PULL_TASK_CONSUME_TIME);
		sb.append(",");
		sb.append(TITLE_SLAVE_PULLED_TASK_COUNT);
		sb.append(",");
		sb.append(TITLE_SLAVE_AVERAGE_PULL_TASK_CONSUME_TIME);
		sb.append(",");
		sb.append(TITLE_SLAVE_AVERAGE_PULLED_TASK_COUNT);
		sb.append(",");
		sb.append(TITLE_SLAVE_AVERAGE_TASK_CONSUME_TIME);
		sb.append(",");
		sb.append(TITLE_SLAVE_EXECUTE_TASK_CONSUME_TIME);
		sb.append(",");
		sb.append(TITLE_SLAVE_EXECUTEED_TASK_COUNT);
		sb.append(",");
		sb.append(TITLE_SLAVE_CONSUME_DATA_SIZE);
		sb.append(",");
		sb.append(TITLE_SLAVE_CONSUME_DATA_LINE);
		sb.append(",");
		sb.append(TITLE_SLAVE_CONSUME_EMPTY_LINE);
		sb.append(",");
		sb.append(TITLE_SLAVE_CONSUME_EXCEPTION_LINE);
		sb.append(",");
		sb.append(TITLE_SLAVE_KEY_COUNT);
		sb.append(",");
		sb.append(TITLE_SLAVE_VALUE_COUNT);
		sb.append(",");
		sb.append(TITLE_SLAVE_MERGED_TASK_COUNT);
		sb.append(",");
		sb.append(TITLE_SLAVE_MERGED_TASK_CONSUME_TIME);
		return sb.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(super.toString());
		sb.append(",");
		sb.append(System.currentTimeMillis() - slaveStartupTime.getTime());
		sb.append(",");
		sb.append(slavePulledTaskCount);
		sb.append(",");
		sb.append(averagePullTaskConsumeTime);
		sb.append(",");
		sb.append(averagePulledTaskCount);
		sb.append(",");
		sb.append(averageTaskConsumeTime);
		sb.append(",");
		sb.append(slaveExecuteTaskTime.get());
		sb.append(",");
		sb.append(slaveExecutedTaskCount.get());
		sb.append(",");
		sb.append(slaveConsumeDataSize.get());
		sb.append(",");
		sb.append(slaveConsumeDataLine.get());
		sb.append(",");
		sb.append(slaveConsumeEmptyLine.get());
		sb.append(",");
		sb.append(slaveConsumeExceptionLine.get());
		sb.append(",");
		sb.append(keyCount.get());
		sb.append(",");
		sb.append(valueCount.get());
		sb.append(",");
		sb.append(mergedTaskCount.get());
		sb.append(",");
		sb.append(megeredTaskConsumeTime.get());
		return sb.toString();
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		SlaveMonitorInfo clone = (SlaveMonitorInfo)super.clone();
		clone.slaveStartupTime = slaveStartupTime;
		clone.slavePulledTaskCount = slavePulledTaskCount;
		clone.averagePullTaskConsumeTime = averagePullTaskConsumeTime;
		clone.averagePulledTaskCount = averagePulledTaskCount;
		clone.averageTaskConsumeTime = averageTaskConsumeTime;
		clone.slaveExecuteTaskTime = new AtomicLong(slaveExecuteTaskTime.get());
		clone.slaveExecutedTaskCount = new AtomicLong(slaveExecutedTaskCount.get());
		clone.slaveConsumeDataSize = new AtomicLong(slaveConsumeDataSize.get());
		clone.slaveConsumeDataLine = new AtomicLong(slaveConsumeDataLine.get());
		clone.slaveConsumeEmptyLine = new AtomicLong(slaveConsumeEmptyLine.get());
		clone.slaveConsumeExceptionLine = new AtomicLong(slaveConsumeExceptionLine.get());
		clone.keyCount = new AtomicLong(keyCount.get());
		clone.valueCount = new AtomicLong(valueCount.get());
		clone.megeredTaskConsumeTime = new AtomicLong(mergedTaskCount.get());
		clone.megeredTaskConsumeTime = new AtomicLong(megeredTaskConsumeTime.get());
		return clone;
	}
}
