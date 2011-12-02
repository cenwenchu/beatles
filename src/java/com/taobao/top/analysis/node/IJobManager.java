/**
 * 
 */
package com.taobao.top.analysis.node;


import java.util.Map;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.component.MasterNode;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.job.Job;

/**
 * 任务管理类，是Master的核心类，负责对于整个任务管理流程
 * 注意由于默认的master是单线程执行任务管理，因此要保证jobManager的各个方法都能够快速返回避免影响流程
 * 同时由于单线程管理，对于任务管理和分配的并发控制就可以比较简单处理
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobManager extends IComponent<MasterConfig>{
	
	/**
	 * 包括重新载入任务列表，检查Task执行状况判断是否需要重置，检查Job状态判断是否需要导出，重置，合并结果
	 * @throws AnalysisException
	 */
	public void checkJobStatus() throws AnalysisException;
	
	/**
	 * 获取没有完成的任务
	 * @param 个数
	 */
	public void getUnDoJobTasks(GetTaskRequestEvent jobRequestEvent);
	
	/**
	 * 增加Slave计算后的返回结果
	 * @param 分析后结果
	 */
	public void addTaskResultToQueue(SendResultsRequestEvent jobResponseEvent);
	
	/**
	 * 导出内存任务数据到磁盘
	 * @param 需要导出的任务
	 */
	public void exportJobData(String jobName);
	
	/**
	 * 从磁盘载入任务数据到内存，Job主干
	 * @param 需要导入的任务
	 */
	public void loadJobData(String jobName);
	
	/**
	 * 从磁盘载入任务数据到内存,非Job主干
	 * @param 需要导入的任务
	 */
	public void loadJobDataToTmp(String jobName);
	
	/**
	 * 清除掉某一个job在内存的数据
	 * @param 任务名称
	 */
	public void clearJobData(String jobName);
	
	/**
	 * 设置masterNode来向上传递消息，由master向另一些部件发起消息或者处理
	 * @param masterNode
	 */
	public void setMasterNode(MasterNode masterNode);
	
	public IJobBuilder getJobBuilder();
	
	public void setJobBuilder(IJobBuilder jobBuilder);
	
	public IJobExporter getJobExporter();
	
	public void setJobExporter(IJobExporter jobExporter);
	
	public IJobResultMerger getJobResultMerger();
	
	public void setJobResultMerger(IJobResultMerger jobResultMerger);
	
	public Map<String,Job> getJobs();
	
	public void setJobs(Map<String,Job> jobs);
	
	
}
