/**
 * 
 */
package com.taobao.top.analysis.node.operation;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobMergedResult;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;


/**
 * 合并任务操作，支持主干和分支两种合并模式
 * @author fangweng
 * @Email  fangweng@taobao.com
 * 2011-11-29
 *
 */
public class MergeJobOperation implements Runnable {

	private static final Log logger = LogFactory.getLog(MergeJobOperation.class);
	
	private Job job;
	private int mergeCount = 0;
	private List<Map<String, Map<String, Object>>> mergeResults;
	private MasterConfig config;
	private BlockingQueue<JobMergedResult> branchResultQueue;

	public MergeJobOperation(Job job,int mergeCount,
			List<Map<String, Map<String, Object>>> mergeResults
			,MasterConfig config,BlockingQueue<JobMergedResult> branchResultQueue) {
		this.job = job;
		this.mergeCount = mergeCount;
		this.mergeResults = mergeResults;
		this.config = config;
		this.branchResultQueue = branchResultQueue;
	}

	@Override
	public void run() {

		long beg = System.currentTimeMillis();
		
		// 尝试获取锁，如果失败先合并其他结果最后通过锁来合并主干
		boolean gotIt = job.getTrunkLock().writeLock().tryLock();

		try 
		{
			// 和主干内容一起合并
			if (gotIt) 
				mergeTrunk(beg);
			else
				mergeBranch(beg);

		} catch (Exception ex) {
			logger.error("MergeJobTask execute error", ex);
		} finally {
			if (gotIt)
				job.getTrunkLock().writeLock().unlock();
		}
	}
	
	void mergeBranch(long beg)
	{
		// 开始中间结果合并		
		logger.warn(new StringBuilder(
				"==>Start noTrunk merge,instance:"
						+ job.getJobName())
				.append(".merge count : ").append(mergeCount)
				.append(", total merged count: ")
				.append(job.getMergedTaskCount()));

		int size = mergeResults.size();
		@SuppressWarnings("unchecked")
		Map<String, Map<String, Object>>[] results = new java.util.HashMap[size];
		for (Map<String, Map<String, Object>> r : mergeResults) {
			size -= 1;
			results[size] = r;
		}

		Map<String, Map<String, Object>> otherResult;

		if (mergeResults.size() == 1)
			otherResult = results[0];
		else
			otherResult = ReportUtil.mergeEntryResult(results, job.getStatisticsRule().getEntryPool(), false);

		// 将结果放入到队列中等待获得锁的线程去执行
		JobMergedResult jr = new JobMergedResult();
		jr.setMergeCount(mergeCount);
		jr.setMergedResult(otherResult);
		branchResultQueue.offer(jr);
		
		logger.warn(new StringBuilder(
				"==>End noTrunk merge,instance:"
						+ job.getJobName())
				.append(",once merge consume : ")
				.append(System.currentTimeMillis() - beg)
				.toString());

		results = null;
		mergeResults.clear();
	
	}
	
	void mergeTrunk(long beg) throws InterruptedException
	{
		int size = mergeResults.size();
		boolean flag = false;
		
		Map<String, Map<String, Object>>  diskTmpResult = null;
		
		//已经到了最后一轮合并
		if (job.getMergedTaskCount().addAndGet(mergeCount) == job.getTaskCount())
		{
			//磁盘换内存模式
			if (config.getSaveTmpResultToFile())
			{
				if (job.getNeedLoadResultFile().compareAndSet(true, false))
				{
					new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_LOADDATA_TO_TMP,this.config).run();
				}
				
				boolean gotLock = job.getLoadLock().tryLock(80, TimeUnit.SECONDS);
				
				if (gotLock)
				{
					try
					{
						diskTmpResult = job.getDiskResult();
						job.setDiskResult(null);
					}
					finally
					{
						job.getLoadLock().unlock();
					}
				}
				else
				{
					throw new java.lang.RuntimeException("load Disk Result Error! check now!!!");
				}
				
				
				if (diskTmpResult != null)
					size += 1;
			}
		}
		else
		{
			if (!config.getSaveTmpResultToFile() &&
					job.getJobResult() == null)
			{
				new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_LOADDATA,this.config).run();
			}
		}
		
		if (job.getJobResult() != null
				&& job.getJobResult().size() > 0) {
			flag = true;
			size += 1;
		}

		@SuppressWarnings("unchecked")
		Map<String, Map<String, Object>>[] results = new java.util.HashMap[size];

		if (flag)
			results[0] = job.getJobResult();
		
	   if (diskTmpResult != null)
	   {
		   if (flag)
			   results[1] = diskTmpResult;
		   else
			   results[0] = diskTmpResult;
		   
	   }

		for (Map<String, Map<String, Object>> r : mergeResults) {
			size -= 1;
			results[size] = r;
		}
					

		logger.warn(new StringBuilder(
				"==>Start Trunk merge,instance:"
						+ job.getJobName())
				.append(".merge count : ").append(mergeCount)
				.append(", total merged count: ")
				.append(job.getMergedTaskCount()).toString());
		
		job.setJobResult(ReportUtil.mergeEntryResult(results, job.getStatisticsRule().getEntryPool(), false));

		logger.warn(new StringBuilder(
				"==>End Trunk merge,instance:"
						+ job.getJobName())
				.append(",once merge consume : ")
				.append(System.currentTimeMillis() - beg)
				.toString());
		
		//全部合并结束，后续可以输出数据了
		if (job.getMergedTaskCount().get() == job.getTaskCount())
			job.isMerged().set(true);
		
		results = null;
		mergeResults.clear();
	}


}
