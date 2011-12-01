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
 * 任务结果合并接口，用于提供不同方式的结果合并
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobResultMerger extends IComponent<MasterConfig>{
	
	/**
	 * 用于Master的合并
	 * 合并某一个job的所有可以合并的结果，当前默认实现是多线程合并（主干只是单线程竞争合并）
	 * @param job
	 * @param 分支队列，支持多个线程同时合并，不需要等待主干合并结束顺序化合并
	 * @param 需要合并的结果集合
	 * @param 是否需要合并的时候处理一些lazy的内容（基于entry的再次计算）
	 */
	public void merge(Job job,BlockingQueue<JobMergedResult> branchResultQueue
			,BlockingQueue<JobTaskResult> jobTaskResultsQueue,boolean needMergeLazy);
	
	/**
	 * 用于Slave的合并
	 * 合并某一个Task的多个结果
	 * @param jobTask
	 * @param jobTaskResults
	 * @param needMergeLazy
	 * @return
	 */
	public JobTaskResult merge(JobTask jobTask,List<JobTaskResult> jobTaskResults,boolean needMergeLazy);
	
}
