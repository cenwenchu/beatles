/**
 * 
 */
package com.taobao.top.analysis.node;

import java.util.List;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobResultMerger extends IComponent<MasterConfig>{
	
	public void merge(Job job,boolean needMergeLazy);
	
	public JobTaskResult merge(JobTask jobTask,List<JobTaskResult> jobTaskResults,boolean needMergeLazy);

}
