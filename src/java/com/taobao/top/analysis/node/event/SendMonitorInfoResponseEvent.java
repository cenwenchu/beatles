package com.taobao.top.analysis.node.event;

import com.taobao.top.analysis.node.monitor.MasterMonitorInfo;

/**
 * 响应Slave提交监控信息的事件
 * @author sihai
 * @Email sihai@taobao.com
 * 2012-05-15
 *
 */
public class SendMonitorInfoResponseEvent extends SlaveNodeEvent {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3158143465435664107L;

	/**
	 * Master的监控信息
	 */
	private MasterMonitorInfo masterMonitorInfo;
	
	public SendMonitorInfoResponseEvent(String sequence) {
		this.sequence = sequence;
		this.eventCode = SlaveEventCode.SEND_MONITOR_INFO_RESP;
	}
	
	public MasterMonitorInfo getMasterMonitorInfo() {
		return masterMonitorInfo;
	}

	public void setMasterMonitorInfo(MasterMonitorInfo masterMonitorInfo) {
		this.masterMonitorInfo = masterMonitorInfo;
	}
}
