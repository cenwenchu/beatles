/**
 * 
 */
package com.taobao.top.analysis.node.master;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
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
	public void processEvent(MasterNodeEvent event) {
		// TODO Auto-generated method stub
		
	}

}
