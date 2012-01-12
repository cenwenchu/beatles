/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobResultMerger;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobMergedResult;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.node.operation.JobDataOperation;
import com.taobao.top.analysis.node.operation.MergeJobOperation;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.NamedThreadFactory;
import com.taobao.top.analysis.util.ReportUtil;


/**
 * 任务合并接口的实现
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-30
 *
 */
public class JobResultMerger implements IJobResultMerger {

	private final Log logger = LogFactory.getLog(JobResultMerger.class);
	
	MasterConfig config;
	
	/**
	 * 用于合并结果集的线程池
	 */
	private ThreadPoolExecutor mergeJobResultThreadPool;
	
	int maxMergeJobWorker = 2;
	
	
	public int getMaxMergeJobWorker() {
		return maxMergeJobWorker;
	}


	public void setMaxMergeJobWorker(int maxMergeJobWorker) {
		this.maxMergeJobWorker = maxMergeJobWorker;
	}


	@Override
	public void init() throws AnalysisException {
		
		if (config != null)
			maxMergeJobWorker = config.getMaxMergeJobWorker();
		
		mergeJobResultThreadPool = new ThreadPoolExecutor(
				maxMergeJobWorker,
				maxMergeJobWorker, 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("mergeJobResult_worker"));
		
		if (logger.isInfoEnabled())
			logger.info("JobResultMerger init end. maxMergeJobWorker size : " + maxMergeJobWorker);
	}

	
	@Override
	public void releaseResource() {
		mergeJobResultThreadPool.shutdown();
	}

	
	@Override
	public MasterConfig getConfig() {
		return config;
	}

	
	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}

	
	@Override
	public void merge(Job job,BlockingQueue<JobMergedResult> branchResultQueue
			,BlockingQueue<JobTaskResult> jobTaskResultsQueue,boolean needMergeLazy) {
		
		if (logger.isInfoEnabled())
			logger.info("start merge check jobName : " + job.getJobName());
		
		// 检查job列表
		List<Map<String, Map<String, Object>>> mergeResults = new ArrayList<Map<String, Map<String, Object>>>();
		int mergeResultCount = 0;

		long collectJobTime = System.currentTimeMillis();

		// 小于批量操作的数目,实际数目
		while (mergeResults.size() < config
				.getMinMergeJobCount()) {
			
			JobTaskResult jt = jobTaskResultsQueue.poll();

			while (jt != null) {
				mergeResults.add(jt.getResults());
				mergeResultCount += jt.getTaskIds().size();
				jt = jobTaskResultsQueue.poll();
			}

			
			JobMergedResult jr = branchResultQueue.poll();

			// 将未何并到主干的结果也继续交给线程去做合并
			while (jr != null) {
				mergeResults.add(jr.getMergedResult());
				mergeResultCount += jr.getMergeCount();
				jr = branchResultQueue.poll();
			}
			
			// 最后一拨需要合并的数据,不需要再等待批量去做
			if (job.getMergedTaskCount().get() + mergeResultCount >= job.getTaskCount())
				break;

			if (System.currentTimeMillis() - collectJobTime > config.getMaxJobResultBundleWaitTime())
				break;
			
			// 放缓一些节奏
			if (mergeResultCount == 0) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {}
			}
		}
		
		if (logger.isInfoEnabled())
			logger.info("jobName : " + job.getJobName() + ", got " + mergeResultCount + " need to merge");
		
		//判断是否可以开始载入外部磁盘换存储的文件,大于AsynLoadDiskFilePrecent的时候开始载入数据等待分析
		if (config.getSaveTmpResultToFile())
			if (job.getMergedTaskCount().get() * 100 
					/ job.getTaskCount() >= config.getAsynLoadDiskFilePrecent())
			{		
				if (logger.isInfoEnabled())
					logger.info("start asyn load " + job.getJobName() + " trunkData from disk");
				
				if (job.getNeedLoadResultFile().compareAndSet(true, false))
				{
					new Thread(new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_LOADDATA_TO_TMP,this.config)).start();
				}
			}

		
		if (mergeResultCount > 0) 
		{			
				mergeJobResultThreadPool
						.execute(new MergeJobOperation(job,
								mergeResultCount,
								mergeResults,config,branchResultQueue));
		}
		else
		{
			// 放缓一点节奏
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
			}
		}
	}

	
	@Override
	public JobTaskResult merge(JobTask jobTask,
			List<JobTaskResult> jobTaskResults,boolean needMergeLazy) {
		
		if (jobTaskResults == null || (jobTaskResults != null && jobTaskResults.size() == 0))
			return null;
		
		if (jobTaskResults.size() == 1)
			return jobTaskResults.get(0);
		
		if (logger.isInfoEnabled())
		{
			StringBuilder info = new StringBuilder("start merge check jobTask : ");
			
			for(JobTaskResult taskResult : jobTaskResults)
				for(String id : taskResult.getTaskIds())
					info.append(id).append(" , ");
			
			logger.info(info.toString());
		}
		
		JobTaskResult base = jobTaskResults.get(0);
		
		@SuppressWarnings("unchecked")
		Map<String, Map<String, Object>>[] taskResultContents = new Map[jobTaskResults.size()];
		taskResultContents[0] = base.getResults();
		
		for(int i = 1 ; i < jobTaskResults.size(); i++)
		{
			JobTaskResult mergeResult = jobTaskResults.get(i);
			
			taskResultContents[i] = mergeResult.getResults();
			
			base.addTaskIds(mergeResult.getTaskIds());
			base.addTaskExecuteInfos(mergeResult.getTaskExecuteInfos());
		}
		
		base.setResults(ReportUtil.mergeEntryResult(taskResultContents, 
				jobTask.getStatisticsRule().getEntryPool(), needMergeLazy));
		
		return base;
	}

}
