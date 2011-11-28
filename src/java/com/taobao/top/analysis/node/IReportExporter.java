/**
 * 
 */
package com.taobao.top.analysis.node;

import java.util.List;

import com.taobao.top.analysis.job.JobTask;

/**
 * 报表导出接口，根据执行后的任务，导出数据
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
