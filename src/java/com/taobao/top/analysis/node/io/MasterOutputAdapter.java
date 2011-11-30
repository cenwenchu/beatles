/**
 * 
 */
package com.taobao.top.analysis.node.io;


import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;

/**
 * 返回到Master的适配器
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class MasterOutputAdapter implements IOutputAdaptor{

	@Override
	public boolean ignore(String output) {
		return output.indexOf("master:") < 0;
	}

	@Override
	public void sendResultToOutput(JobTask jobTask,JobTaskResult jobTaskResult) {
		// TODO Auto-generated method stub
		
	}

}
