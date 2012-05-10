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
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobMergedResult;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;
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
			{
				mergeTrunk(beg);
				job.getJobMergeTime().addAndGet(System.currentTimeMillis() - beg);
			}
			else
			{
				mergeBranch(beg);
				job.getJobMergeBranchCount().incrementAndGet();
			}

		} catch (Exception ex) {
			logger.error("MergeJobTask execute error", ex);
		} finally {
			if (gotIt)
				job.getTrunkLock().writeLock().unlock();
		}
	}
	
	void mergeBranch(long beg)
	{
		int epoch = job.getEpoch().get();
		
		if (job.getJobTimeOut().get())
			return;
		
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
			otherResult = ReportUtil.mergeEntryResult(results, job.getStatisticsRule().getEntryPool(), false,ReduceType.SHALLOW_MERGE);

		//对于timeout引起的reset做一层保护，丢弃掉分支合并的结果
		if (job.getEpoch().get() == epoch)
		{
			// 将结果放入到队列中等待获得锁的线程去执行
			JobMergedResult jr = new JobMergedResult();
			jr.setMergeCount(mergeCount);
			jr.setMergedResult(otherResult);
			branchResultQueue.offer(jr);
		}
		
		logger.warn(new StringBuilder(
				"==>End noTrunk merge,instance:"
						+ job.getJobName())
				.append(",once merge consume : ")
				.append(System.currentTimeMillis() - beg)
				.toString());

		results = null;
		mergeResults.clear();
	
	}
	
	public static boolean  mergeToTrunk(Job job,
			List<Map<String, Map<String, Object>>> mergeResults, MasterConfig config)
	{
		boolean gotIt = false;
		
		try
		{
			gotIt = job.getTrunkLock().writeLock().tryLock(1, TimeUnit.MINUTES);
			boolean flag = false;
			
			if (gotIt)
			{
				int size = mergeResults.size();
				long beg = System.currentTimeMillis();
				
                if (config.getSaveTmpResultToFile()) {

                    if (job.getJobResult() != null && job.getJobResult().size() > 0) {
                        flag = true;
                        size += 1;
                    }
                }
				
				@SuppressWarnings("unchecked")
				Map<String, Map<String, Object>>[] results = new java.util.HashMap[size];

				if (flag)
					results[0] = job.getJobResult();
				
				for (Map<String, Map<String, Object>> r : mergeResults) {
					size -= 1;
					results[size] = r;
				}
				
				logger.warn(new StringBuilder(
						"==>Start Trunk merge(data recover),instance:"
								+ job.getJobName())
						.append(".merge count : ").append(size)
						.append(", total merged count: ")
						.append(job.getMergedTaskCount()).toString());
				
				job.setJobResult(ReportUtil.mergeEntryResult(results, job.getStatisticsRule().getEntryPool(), false,ReduceType.DEEP_MERGE));

				logger.warn(new StringBuilder(
						"==>End Trunk merge(data recover),instance:"
								+ job.getJobName())
						.append(",once merge consume : ")
						.append(System.currentTimeMillis() - beg)
						.toString());
				
				results = null;
				mergeResults.clear();
			}
			else
			{
				logger.error("can't got trunk to load recover data.");
			}
		}
		catch(InterruptedException ex)
		{
			//do nothing
		}
		finally
		{
			if(gotIt)
				job.getTrunkLock().writeLock().unlock();
		}
		
		return gotIt;
		
	}
	
	void mergeTrunk(long beg) throws InterruptedException
	{
		int size = mergeResults.size();
		boolean flag = false;
		if(job.isMerged().get())
		    return;
		
		Map<String, Map<String, Object>>  diskTmpResult = null;
		
		//已经到了最后一轮合并
		if (job.getMergedTaskCount().addAndGet(mergeCount) == job.getTaskCount() || job.getJobTimeOut().get())
		{
			//磁盘换内存模式
			if (config.getSaveTmpResultToFile())
			{
				if (job.getNeedLoadResultFile().compareAndSet(true, false))
				{
				    try {
                        JobDataOperation.loadDataToTmp(job, config);
                    }
                    catch (AnalysisException e) {
                        logger.error("loadDataToTmp error.",e);
                    }
				}
				
				boolean gotLock = job.getLoadLock().tryLock(80, TimeUnit.SECONDS);
				
				logger.warn("merge diskResult of " + job.getJobName());
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
				    logger.warn("load Disk Result Error! check now!!!");
					throw new java.lang.RuntimeException("load Disk Result Error! check now!!!");
				}
				
				if(job.getDiskResult() == null)
				    job.setDiskResultMerged(true);
				
				if (diskTmpResult != null)
					size += 1;
			}
		}
		else
		{
			if (!config.getSaveTmpResultToFile() &&
					job.getJobResult() == null)
			{
				try {
                    JobDataOperation.loadData(job, config);
                }
                catch (AnalysisException e) {
                    logger.error("loadData error.",e);
                }
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
		
		job.setJobResult(ReportUtil.mergeEntryResult(results, job.getStatisticsRule().getEntryPool(), false,ReduceType.DEEP_MERGE));

		logger.warn(new StringBuilder(
				"==>End Trunk merge,instance:"
						+ job.getJobName())
				.append(",once merge consume : ")
				.append(System.currentTimeMillis() - beg)
				.toString());
		
		boolean checkDisk = true;
		if(config.getSaveTmpResultToFile())
		    checkDisk = job.isDiskResultMerged();
		
		//全部合并结束，后续可以输出数据了
		if (job.getMergedTaskCount().get() == job.getTaskCount() || (job.getJobTimeOut().get() && checkDisk))
			job.isMerged().set(true);
		
		results = null;
		mergeResults.clear();
	}


}
