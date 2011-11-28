package com.taobao.top.analysis.node.base.impl;


import java.util.List;
import org.junit.Test;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.job.Job;
import com.taobao.top.analysis.job.JobTask;
import com.taobao.top.analysis.node.IInputAdaptor;
import com.taobao.top.analysis.node.impl.DefaultAnalysisEngine;
import com.taobao.top.analysis.node.impl.FileJobExporter;
import com.taobao.top.analysis.node.impl.FileInputAdaptor;
import com.taobao.top.analysis.node.impl.FileOutputAdaptor;
import com.taobao.top.analysis.node.impl.HttpInputAdaptor;
import com.taobao.top.analysis.node.impl.FileJobBuilder;


/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-26
 * 
 * 测试整个分析统计抽象层的测试用例子，analysis-job-config.properties中的一个数据来源
 * 需要通过http本地获得数据，因此首先跑这个用例以前先运行TestServer,然后再跑这个例子
 *
 */
public class DefaultAnalysisEngineTest {

	@Test
	public void testDoAnalysis() throws Exception {
		DefaultAnalysisEngine defaultAnalysisEngine = new DefaultAnalysisEngine();
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		FileOutputAdaptor fileOutAdaptor = new FileOutputAdaptor();
		MasterConfig masterConfig = new MasterConfig();
		
		FileJobExporter fileJobExporter = new FileJobExporter();
		fileJobExporter.setConfig(masterConfig);
		fileJobExporter.init();
		
		fileOutAdaptor.setJobExporter(fileJobExporter);
		
		defaultAnalysisEngine.addInputAdaptor(fileInputAdaptor);
		defaultAnalysisEngine.addInputAdaptor(httpInputAdaptor);
		defaultAnalysisEngine.addOutputAdaptor(fileOutAdaptor);
		
		
		FileJobBuilder jobBuilder = new FileJobBuilder();
		List<Job> jobs = jobBuilder.build("jobs-config.properties");
		
		for(Job job : jobs)
		{
			List<JobTask> tasks = job.getJobTasks();
			
			for(JobTask jobtask : tasks)
				defaultAnalysisEngine.doAnalysis(jobtask);
		}

		fileJobExporter.releaseResource();
		
	}

}
