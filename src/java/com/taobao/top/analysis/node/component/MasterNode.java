/**
 * 
 */
package com.taobao.top.analysis.node.component;



import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
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
import com.taobao.top.analysis.util.AnalyzerZKWatcher;
import com.taobao.top.analysis.util.ZKUtil;

/**
 * 分布式集群 Master Node （可以是虚拟机内部的）
 * 使用方式参考MasterSlaveIntegrationTest 类
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class MasterNode extends AbstractNode<MasterNodeEvent,MasterConfig> {

	private static final Log logger = LogFactory.getLog(MasterNode.class);
	
	/**
	 * 任务管理组件
	 */
	private IJobManager jobManager;
	/**
	 * 用于与Slave通信的组件
	 */
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
		
		//增加一块对于zookeeper的支持
		if (StringUtils.isNotEmpty(config.getZkServer()))
		{
			try
			{
				AnalyzerZKWatcher<MasterConfig> analyzerZKWatcher = 
						new AnalyzerZKWatcher<MasterConfig>(config);
				zk = new ZooKeeper(config.getZkServer(),3000,analyzerZKWatcher);
				analyzerZKWatcher.setZk(zk);
				
				//每次启动时都先检查是否有根目录
				ZKUtil.createGroupNodesIfNotExist(zk,config.getGroupId());
				
				ZKUtil.updateOrCreateNode(zk,ZKUtil.getGroupMasterZKPath(config.getGroupId())
						+ "/" + config.getMasterName(),config.marshal().getBytes("UTF-8"));
				
			}
			catch(Exception ex)
			{
				logger.error("config to zk error!",ex);
			}
			
		}
		
		if (logger.isInfoEnabled())
			logger.info("Master init complete.");
	}

	@Override
	public void releaseResource() {
		if (jobManager != null)
			jobManager.releaseResource();
		
		if (masterConnector != null)
			masterConnector.releaseResource();
		
		//增加一块对于zookeeper的支持
		if (StringUtils.isNotEmpty(config.getZkServer()) && zk != null)
		{
			try
			{
				ZKUtil.deleteNode(zk,ZKUtil.getGroupMasterZKPath(config.getGroupId())
						+ "/" + config.getMasterName());
				
			}
			catch(Exception ex)
			{
				logger.error("delete zk node error!",ex);
			}
			
		}
		
		if (logger.isInfoEnabled())
			logger.info("Master releaseResource complete.");
	}

	@Override
	public void process() throws AnalysisException {
	    try {
	        jobManager.checkJobStatus();
	    } catch (Throwable e) {
	        logger.error(e);
	    }
	}
	
	/**
	 * 响应请求任务的逻辑
	 * @param sequence
	 * @param jobTasks
	 */
	public void echoGetJobTasks(String sequence,List<JobTask> jobTasks,Object channel)
	{
		GetTaskResponseEvent event = new GetTaskResponseEvent(sequence);
		event.setJobTasks(jobTasks);
		event.setChannel(channel);
		
		masterConnector.echoGetJobTasks(event);
	}
	
	/**
	 * 响应Slave提交任务结果的请求
	 * @param 返回结果
	 */
	public void echoSendJobTaskResults(String sequence,String response,Object channel)
	{
		SendResultsResponseEvent event = new SendResultsResponseEvent(sequence);
		event.setResponse(response);
		event.setChannel(channel);
		
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
				break;
				
			case LOAD_BACKUP_DATA:
				jobManager.loadJobBackupData(((JobManageEvent)event).getJobName()
						,(String)((JobManageEvent)event).getAttachment());
				if (logger.isInfoEnabled())
					logger.info("Master process LOAD_BACKUP_DATA Event");
				break;
				
			case SUSPEND:
				this.suspendNode();
				break;
				
			case AWAKE:
				this.awaitNode();
				break;
				
			case RESETSERVER:
			    this.getMasterConnector().openServer();
			    break;
				
			default:
				throw new AnalysisException("Not support such Event : " + event.getEventCode().toString());
		}
	}

}
