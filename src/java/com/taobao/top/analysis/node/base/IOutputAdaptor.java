/**
 * 
 */
package com.taobao.top.analysis.node.base;

import com.taobao.top.analysis.job.JobTask;


/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public interface IOutputAdaptor {

	public void sendResultToOutput(JobTask jobtask);
	
	boolean ignore(String output);
	
}
