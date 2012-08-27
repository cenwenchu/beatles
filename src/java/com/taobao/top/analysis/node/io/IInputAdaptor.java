/**
 * 
 */
package com.taobao.top.analysis.node.io;


import java.io.InputStream;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;

/**
 * 输入适配器，用于根据任务定义获得数据来源（支持文件，http，hdfs等等随意扩展）
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public interface IInputAdaptor {
	
	InputStream getInputFormJob(JobTask jobtask, JobTaskExecuteInfo taskExecuteInfo);
	
	boolean ignore(String input);

}
