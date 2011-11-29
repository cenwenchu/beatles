/**
 * 
 */
package com.taobao.top.analysis.statistics;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import com.taobao.top.analysis.job.JobTask;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.io.IOutputAdaptor;

/**
 * 每个计算节点的抽象接口，可以植入多种输入适配器，输出适配器
 * 执行任务时，根据输入适配器获得数据来源，然后根据任务自我业务分析规则分析数据，
 * 最后根据输出适配器来输出到外部一个或者多个数据输出点
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public interface IStatisticsEngine{
	
	public void addInputAdaptor(IInputAdaptor inputAdaptor);
	public void removeInputAdaptor(IInputAdaptor inputAdaptor);
	
	public void addOutputAdaptor(IOutputAdaptor outputAdaptor);
	public void removeOutputAdaptor(IOutputAdaptor outputAdaptor);
	
	public void doAnalysis(JobTask jobTask) throws UnsupportedEncodingException,IOException;

}
