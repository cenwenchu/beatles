/**
 * 
 */
package com.taobao.top.analysis.node.impl;



import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.AbstractNode;
import com.taobao.top.analysis.node.IJobManager;
import com.taobao.top.analysis.node.event.JobManageEvent;
import com.taobao.top.analysis.node.event.JobRequestEvent;
import com.taobao.top.analysis.node.event.JobResponseEvent;
import com.taobao.top.analysis.node.event.MasterNodeEvent;

/**
 * Master
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class MasterNode extends AbstractNode<MasterNodeEvent,MasterConfig> {

	private IJobManager jobManager;
	
	public IJobManager getJobManager() {
		return jobManager;
	}

	public void setJobManager(IJobManager jobManager) {
		this.jobManager = jobManager;
	}

	@Override
	public void init() throws AnalysisException {
		jobManager.init();				
	}

	@Override
	public void releaseResource() {
		jobManager.releaseResource();
	}

	@Override
	public void process() throws AnalysisException {
		jobManager.checkJobStatus();
	}

	@Override
	public void processEvent(MasterNodeEvent event) throws AnalysisException {
		
		switch (event.getEventCode())
		{
			case GET_TASK:
				jobManager.getUnDoJobTasks((JobRequestEvent)event);
				break;
		
			case SEND_RESULT:
				jobManager.addTaskResultToQueue((JobResponseEvent)event);
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
