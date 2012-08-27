/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.node.IComponent;
import com.taobao.top.analysis.node.component.SlaveNode;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.SendMonitorInfoEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.monitor.MasterMonitorInfo;

/**
 * Slave端的通信组件，主要用于服务端与客户端通信，可自定义扩展为内存交互，socket交互，db交互等
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public interface ISlaveConnector extends IComponent<SlaveConfig>{
	
	/**
	 * 请求任务
	 * @param 请求任务参数
	 * @return
	 */
	public JobTask[] getJobTasks(GetTaskRequestEvent requestEvent);

	/**
	 * 发送分析后的结果
	 * @param 分析后的结果
	 * @param 支持多个master分担处理结果
	 * @return
	 */
	public String sendJobTaskResults(SendResultsRequestEvent jobResponseEvent,String master);
	
	/**
	 * 向Master发送Slave监控信息
	 */
	public MasterMonitorInfo sendMonitorInfo(SendMonitorInfoEvent sendSlaveMonitorInfoEvent);
	
	/**
	 * 运行期可以切换主master
	 * @param master
	 */
	public void changeMaster(String master);
	
	/**
	 * 设置slaveNode
	 * @param slaveNode
	 */
	public void setSlaveNode(SlaveNode slaveNode);

}
