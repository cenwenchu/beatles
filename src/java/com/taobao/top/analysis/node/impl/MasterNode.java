/**
 * 
 */
package com.taobao.top.analysis.node.impl;



import java.util.List;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobManager;
import com.taobao.top.analysis.node.connect.IMasterConnector;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.JobManageEvent;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;
import com.taobao.top.analysis.node.job.JobTask;

/**
 * Master
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class MasterNode extends AbstractNode<MasterNodeEvent,MasterConfig> {

	private IJobManager jobManager;
	private IMasterConnector masterConnector;
	
	public IJobManager getJobManager() {
		return jobManager;
	}

	public void setJobManager(IJobManager jobManager) {
		this.jobManager = jobManager;
	}
	
	

	public IMasterConnector getMasterConnector() {
		return masterConnector;
	}

	public void setMasterConnector(IMasterConnector masterConnector) {
		this.masterConnector = masterConnector;
	}

	@Override
	public void init() throws AnalysisException {
		jobManager.setMasterNode(this);
		masterConnector.setMasterNode(this);
		jobManager.init();	
		masterConnector.init();
	}

	@Override
	public void releaseResource() {
		jobManager.releaseResource();
		masterConnector.releaseResource();
	}

	@Override
	public void process() throws AnalysisException {
		jobManager.checkJobStatus();
	}
	
	public void echoGetJobTasks(String sequence,List<JobTask> jobTasks)
	{
		GetTaskResponseEvent event = new GetTaskResponseEvent(sequence);
		event.setJobTasks(jobTasks);
		
		masterConnector.echoGetJobTasks(event);
	}
	
	/**
	 * 响应发送任务结果的请求
	 * @param 返回结果
	 */
	public void echoSendJobTaskResults(String sequence,String response)
	{
		SendResultsResponseEvent event = new SendResultsResponseEvent(sequence);
		event.setResponse(response);
		
		masterConnector.echoSendJobTaskResults(event);
	}


	@Override
	public void processEvent(MasterNodeEvent event) throws AnalysisException {
		
		switch (event.getEventCode())
		{
			case GET_TASK:
				jobManager.getUnDoJobTasks((GetTaskRequestEvent)event);
				break;
		
			case SEND_RESULT:
				jobManager.addTaskResultToQueue((SendResultsRequestEvent)event);
				break;
		
			case RELOAD_JOBS:
				jobManager.getJobBuilder().setNeedRebuild(true);
				break;
		
			case EXPORT_DATA:
				jobManager.exportJobData(((JobManageEvent)event).getJobName());
				break;
		
			case CLEAR_DATA:
				jobManager.clearJobData(((JobManageEvent)event).getJobName());
				break;
				
			case LOAD_DATA:
				jobManager.loadJobData(((JobManageEvent)event).getJobName());
				break;
		
			default:
				throw new AnalysisException("Not support such Event : " + event.getEventCode().toString());
		}
	}

}
