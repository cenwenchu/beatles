/**
 * 
 */
package com.taobao.top.analysis.node.base.impl;

import com.taobao.top.analysis.job.JobTask;
import com.taobao.top.analysis.node.base.IOutputAdaptor;

/**
 * 返回到Master的适配器
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class MasterOutputAdapter implements IOutputAdaptor{

	@Override
	public void sendResultToOutput(JobTask jobtask) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean ignore(String output) {
		return output.indexOf("Master:") < 0;
	}

}
