/**
 * 
 */
package com.taobao.top.analysis.node;


import java.util.Map;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.job.Job;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobManager extends IComponent<MasterConfig>{
	
	
	public IJobBuilder getJobBuilder();
	
	public void setJobBuilder(IJobBuilder jobBuilder);
	
	public IJobExporter getJobExporter();
	
	public void setJobExporter(IJobExporter jobExporter);
	
	public IJobResultMerger getJobResultMerger();
	
	public void setJobResultMerger(IJobResultMerger jobResultMerger);
	
	public Map<String,Job> getJobs();
	
	public void setJobs(Map<String,Job> jobs);
	
	public void checkJobStatus() throws AnalysisException;

}
