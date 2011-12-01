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
 * 任务导出管理类，支持任务输出结果报表，载入导出分析中间结果（保存分析现场，支持容灾）
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobExporter extends IComponent<MasterConfig>{
	
	/**
	 * 导出报表
	 * @param 需要导出的Job
	 * @param 是否需要时间后缀
	 * @return
	 */
	public List<String> exportReport(Job job,boolean needTimeSuffix);
	
	/**
	 * 导出报表
	 * @param 需要导出的任务
	 * @param 需要导出任务对应的结果
	 * @param 是否需要时间后缀
	 * @return
	 */
	public List<String> exportReport(JobTask jobTask,JobTaskResult jobTaskResult,boolean needTimeSuffix);
	
	/**
	 * 导出分析器某一个job主干的中间分析数据
	 * @param job
	 */
	public void exportEntryData(Job job);
	
	/**
	 * 载入分析器某一个job中间分析数据到主干
	 * @param job
	 */
	public void loadEntryData(Job job);
	
	/**
	 * 载入分析器某一个job中间分析数据到job的临时对象中
	 * @param job
	 */
	public void loadEntryDataToTmp(Job job);

}
