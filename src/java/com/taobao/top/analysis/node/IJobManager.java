/**
 * 
 */
package com.taobao.top.analysis.node;


import java.util.Map;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.job.Job;
import com.taobao.top.analysis.node.event.JobRequestEvent;
import com.taobao.top.analysis.node.event.JobResponseEvent;

/**
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
	public void getUnDoJobTasks(JobRequestEvent jobRequestEvent);
	
	/**
	 * 增加Slave计算后的返回结果
	 * @param 分析后结果
	 */
	public void addTaskResultToQueue(JobResponseEvent jobResponseEvent);
	
	/**
	 * 导出内存任务数据到磁盘
	 * @param 需要导出的任务
	 */
	public void exportJobData(String jobName);
	
	/**
	 * 从磁盘载入任务数据到内存
	 * @param 需要导入的任务
	 */
	public void loadJobData(String jobName);
	
	/**
	 * 清除掉某一个job在内存的数据
	 * @param 任务名称
	 */
	public void clearJobData(String jobName);
	
	
	public IJobBuilder getJobBuilder();
	
	public void setJobBuilder(IJobBuilder jobBuilder);
	
	public IJobExporter getJobExporter();
	
	public void setJobExporter(IJobExporter jobExporter);
	
	public IJobResultMerger getJobResultMerger();
	
	public void setJobResultMerger(IJobResultMerger jobResultMerger);
	
	public Map<String,Job> getJobs();
	
	public void setJobs(Map<String,Job> jobs);
	
	
}
