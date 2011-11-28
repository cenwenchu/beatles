/**
 * 
 */
package com.taobao.top.analysis.node;

import java.util.List;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.job.Job;
import com.taobao.top.analysis.job.JobTask;
import com.taobao.top.analysis.job.JobTaskResult;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobExporter extends IComponent<MasterConfig>{
	
	public List<String> export(Job job);
	
	public List<String> export(JobTask jobTask,JobTaskResult jobTaskResult,boolean needTimeSuffix);

}
