/**
 * 
 */
package com.taobao.top.analysis.node.master;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.job.Job;
import com.taobao.top.analysis.node.AbstractNode;
import com.taobao.top.analysis.node.IJobManager;

/**
 * Master
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class MasterNode extends AbstractNode<MasterNodeEvent,MasterConfig> {

	private static final Log logger = LogFactory.getLog(MasterNode.class);
	
	private List<Job> jobs;
	
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
		
		//获得任务数量
		jobs = jobManager.getJobBuilder().build(config.getJobsSource());
		
		if (jobs == null || (jobs != null && jobs.size() == 0))
			throw new AnalysisException("jobs should not be empty!");
					
	}

	@Override
	public void releaseResource() {
		jobs.clear();
		jobManager.releaseResource();
	}

	@Override
	public void process() {
		
		if (jobManager.isNeedReloadJobs())
		{
			List<Job> tmpJobs = null;
			
			try
			{
				tmpJobs = jobManager.getJobBuilder().build(config.getJobsSource());
				
				if (tmpJobs != null && tmpJobs.size() > 0)
				{
					jobs = tmpJobs;
				}
			}
			catch(AnalysisException ex)
			{
				logger.error("reload jobs error.",ex);
			}
			
		}
		
		jobManager.checkJobStatus(jobs);
		
	}

	@Override
	public void processEvent(MasterNodeEvent event) {
		// TODO Auto-generated method stub
		
	}

}
