/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobResultMerger;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.util.ReportUtil;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-30
 *
 */
public class JobResultMerger implements IJobResultMerger {

	MasterConfig config;
	
	@Override
	public void init() throws AnalysisException {
		// TODO Auto-generated method stub

	}

	
	@Override
	public void releaseResource() {
		// TODO Auto-generated method stub

	}

	
	@Override
	public MasterConfig getConfig() {
		// TODO Auto-generated method stub
		return config;
	}

	
	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}

	
	@Override
	public void merge(Job job,boolean needMergeLazy) {
		// TODO Auto-generated method stub

	}

	
	@Override
	public JobTaskResult merge(JobTask jobTask,
			List<JobTaskResult> jobTaskResults,boolean needMergeLazy) {
		
		if (jobTaskResults == null || (jobTaskResults != null && jobTaskResults.size() == 0))
			return null;
		
		if (jobTaskResults.size() == 1)
			return jobTaskResults.get(0);
		
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
