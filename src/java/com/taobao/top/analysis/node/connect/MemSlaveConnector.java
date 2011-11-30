/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;
import com.taobao.top.analysis.node.event.SlaveEventCode;
import com.taobao.top.analysis.node.event.SlaveNodeEvent;
import com.taobao.top.analysis.node.job.JobTask;

/**
 * 用于单机的分布式模拟，用内存作为通信的客户端实现
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class MemSlaveConnector implements ISlaveConnector {

	private final Log logger = LogFactory.getLog(MemSlaveConnector.class);
	
	SlaveConfig config;
	MemTunnel tunnel;
	
	@Override
	public void init() throws AnalysisException {
		// TODO Auto-generated method stub

	}

	
	@Override
	public void releaseResource() {
		// TODO Auto-generated method stub

	}

	
	@Override
	public SlaveConfig getConfig() {
		return config;
	}

	
	@Override
	public void setConfig(SlaveConfig config) {
		this.config = config;
	}

	
	@Override
	//做成阻塞模式
	public JobTask[] getJobTasks(GetTaskRequestEvent requestEvent) {
		
		tunnel.getMasterSide().offer(requestEvent);
		
		try {
			
			SlaveNodeEvent event = tunnel.getSlaveSide().poll(10, TimeUnit.SECONDS);
			
			if (event != null && event.getEventCode().equals(SlaveEventCode.GET_TASK_RESP))
			{
				List<JobTask> jobTasks = ((GetTaskResponseEvent)event).getJobTasks();
				
				JobTask[] result = new JobTask[jobTasks.size()];
				
				jobTasks.toArray(result);
				
				return result;
			}	
			
		} catch (InterruptedException e) {
			logger.error(e);
		}
		
		return null;
	}

	
	@Override
	public String sendJobTaskResults(SendResultsRequestEvent jobResponseEvent) {
		
		tunnel.getMasterSide().offer(jobResponseEvent);
		
		try {
			
			SlaveNodeEvent event = tunnel.getSlaveSide().poll(10, TimeUnit.SECONDS);
			
			if (event != null && event.getEventCode().equals(SlaveEventCode.SEND_RESULT_RESP))
			{
				return ((SendResultsResponseEvent)event).getResponse();
			}	
			
		} catch (InterruptedException e) {
			logger.error(e);
		}
		
		return null;
	}

}
