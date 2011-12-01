/**
 * 
 */
package com.taobao.top.analysis.node.component;



import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	private static final Log logger = LogFactory.getLog(MasterNode.class);
	
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
		jobManager.setConfig(config);
		masterConnector.setMasterNode(this);
		masterConnector.setConfig(config);
		jobManager.init();	
		masterConnector.init();
		
		if (logger.isInfoEnabled())
			logger.info("Master init complete.");
	}

	@Override
	public void releaseResource() {
		jobManager.releaseResource();
		masterConnector.releaseResource();
		
		if (logger.isInfoEnabled())
			logger.info("Master releaseResource complete.");
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
		
		if (logger.isInfoEnabled())
		{
			if (jobTasks != null && jobTasks.size() > 0)
			{
				StringBuilder jobTaskIds = new StringBuilder("Send task to slave, taskids : ");
				
				for(JobTask t : jobTasks)
				{
					jobTaskIds.append(t.getTaskId()).append(",");
				}
				
				logger.info(jobTaskIds.toString());
				
			}
		}
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
				if (logger.isInfoEnabled())
					logger.info("Master process GET_TASK Event");
				break;
		
			case SEND_RESULT:
				jobManager.addTaskResultToQueue((SendResultsRequestEvent)event);
				if (logger.isInfoEnabled())
					logger.info("Master process SEND_RESULT Event");
				break;
		
			case RELOAD_JOBS:
				jobManager.getJobBuilder().setNeedRebuild(true);
				if (logger.isInfoEnabled())
					logger.info("Master process RELOAD_JOBS Event");
				break;
		
			case EXPORT_DATA:
				jobManager.exportJobData(((JobManageEvent)event).getJobName());
				if (logger.isInfoEnabled())
					logger.info("Master process EXPORT_DATA Event");
				break;
		
			case CLEAR_DATA:
				jobManager.clearJobData(((JobManageEvent)event).getJobName());
				if (logger.isInfoEnabled())
					logger.info("Master process CLEAR_DATA Event");
				break;
				
			case LOAD_DATA:
				jobManager.loadJobData(((JobManageEvent)event).getJobName());
				if (logger.isInfoEnabled())
					logger.info("Master process LOAD_DATA Event");
				break;
				
			case LOAD_DATA_TO_TMP:
				jobManager.loadJobDataToTmp(((JobManageEvent)event).getJobName());
				if (logger.isInfoEnabled())
					logger.info("Master process LOAD_DATA_TO_TMP Event");
		
			default:
				throw new AnalysisException("Not support such Event : " + event.getEventCode().toString());
		}
	}

}
