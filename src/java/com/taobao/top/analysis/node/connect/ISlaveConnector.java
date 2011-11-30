/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.node.IComponent;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.job.JobTask;

/**
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
	 * @return
	 */
	public String sendJobTaskResults(SendResultsRequestEvent jobResponseEvent);

}
