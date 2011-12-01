/**
 * 
 */
package com.taobao.top.analysis.node.io;


import java.util.concurrent.atomic.AtomicLong;

import com.taobao.top.analysis.node.connect.ISlaveConnector;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;

/**
 * 返回到Master的适配器
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class MasterOutputAdaptor implements IOutputAdaptor{

	ISlaveConnector slaveConnector;
	AtomicLong sequenceGen = new AtomicLong(0);
	
	
	public ISlaveConnector getSlaveConnector() {
		return slaveConnector;
	}

	public void setSlaveConnector(ISlaveConnector slaveConnector) {
		this.slaveConnector = slaveConnector;
	}

	@Override
	public boolean ignore(String output) {
		return output.indexOf("master:") < 0;
	}

	@Override
	public void sendResultToOutput(JobTask jobTask,JobTaskResult jobTaskResult) {
		// TODO Auto-generated method stub
		
		SendResultsRequestEvent event = new SendResultsRequestEvent(new StringBuilder()
			.append(System.currentTimeMillis()).append("-").append(sequenceGen.incrementAndGet()).toString());
		
		event.setJobTaskResult(jobTaskResult);
		
		slaveConnector.sendJobTaskResults(event);
		
	}

}
