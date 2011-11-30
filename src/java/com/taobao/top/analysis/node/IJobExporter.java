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
public interface IJobExporter extends IComponent<MasterConfig>{
	
	public List<String> exportReport(Job job);
	
	public List<String> exportReport(JobTask jobTask,JobTaskResult jobTaskResult,boolean needTimeSuffix);
	
	public void exportEntryData(Job job);
	
	public void loadEntryData(Job job);

}
