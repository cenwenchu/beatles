/**
 * 
 */
package com.taobao.top.analysis.node.io;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午2:11:52
 *
 */
public class HdfsOutputAdaptor implements IOutputAdaptor {

	
	@Override
	public void sendResultToOutput(JobTask jobTask, JobTaskResult jobTaskResult) {
		// TODO Auto-generated method stub

	}

	
	@Override
	public boolean ignore(String output) {
		return output.indexOf("hdfs:") < 0;
	}

}
