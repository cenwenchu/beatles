package com.taobao.top.analysis.node.event;

import com.taobao.top.analysis.node.monitor.SlaveMonitorInfo;

/**
 * 报告Slave监控信息事件
 * @author sihai
 * @Email sihai@taobao.com
 * 2012-05-14
 *
 */
public class SendMonitorInfoEvent extends MasterNodeEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8235328078816897786L;
	
	/**
	 * Slave监控信息快照
	 */
	private SlaveMonitorInfo slaveMonitorInfo;
	
	public SendMonitorInfoEvent(String sequence) {
		this.sequence = sequence;
		this.eventCode = MasterEventCode.SEND_MONITOR_INFO;
	}
	
	public SlaveMonitorInfo getSlaveMonitorInfo() {
		return slaveMonitorInfo;
	}

	public void setSlaveMonitorInfo(SlaveMonitorInfo slaveMonitorInfo) {
		this.slaveMonitorInfo = slaveMonitorInfo;
	}
}
