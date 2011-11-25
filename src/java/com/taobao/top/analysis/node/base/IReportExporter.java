/**
 * 
 */
package com.taobao.top.analysis.node.base;

import java.util.List;

import com.taobao.top.analysis.job.JobTask;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public interface IReportExporter {
	
	public void init();
	
	public void destory();
	
	public List<String> generateReports(JobTask jobTask,boolean needTimeSuffix);

}
