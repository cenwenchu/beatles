/**
 * 
 */
package com.taobao.top.analysis.node;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobMergedResult;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobResultMerger extends IComponent<MasterConfig>{
	
	public void merge(Job job,BlockingQueue<JobMergedResult> branchResultQueue
			,BlockingQueue<JobTaskResult> jobTaskResultsQueue,boolean needMergeLazy);
	
	public JobTaskResult merge(JobTask jobTask,List<JobTaskResult> jobTaskResults,boolean needMergeLazy);
	
}
