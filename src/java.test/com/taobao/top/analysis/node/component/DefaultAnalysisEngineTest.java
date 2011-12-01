package com.taobao.top.analysis.node.component;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.taobao.top.analysis.node.component.FileJobBuilder;
import com.taobao.top.analysis.node.component.FileJobExporter;
import com.taobao.top.analysis.node.component.JobResultMerger;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.FileOutputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.statistics.StatisticsEngine;


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
		StatisticsEngine defaultAnalysisEngine = new StatisticsEngine();
		defaultAnalysisEngine.init();
		
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		FileOutputAdaptor fileOutAdaptor = new FileOutputAdaptor();
		
		FileJobExporter fileJobExporter = new FileJobExporter();
		fileJobExporter.setMaxCreateReportWorker(2);
		fileJobExporter.init();
		
		fileOutAdaptor.setJobExporter(fileJobExporter);
		
		JobResultMerger jobResultMerger = new JobResultMerger();
		jobResultMerger.init();
		
		defaultAnalysisEngine.addInputAdaptor(fileInputAdaptor);
		defaultAnalysisEngine.addInputAdaptor(httpInputAdaptor);
		defaultAnalysisEngine.addOutputAdaptor(fileOutAdaptor);
			
		FileJobBuilder jobBuilder = new FileJobBuilder();
		Map<String,Job> jobs = jobBuilder.build("jobs-config.properties");
		
		for(Job job : jobs.values())
		{
			List<JobTask> tasks = job.getJobTasks();
			
			List<JobTaskResult> taskResults = new ArrayList<JobTaskResult>();
			
			for(JobTask jobtask : tasks)
			{
				taskResults.add(defaultAnalysisEngine.doAnalysis(jobtask));
			}
			
			JobTaskResult jobTaskResult = jobResultMerger.merge(tasks.get(0), taskResults,true);
			
			defaultAnalysisEngine.doExport(tasks.get(0), jobTaskResult);
		}

		fileJobExporter.releaseResource();
		jobResultMerger.releaseResource();
		defaultAnalysisEngine.releaseResource();
		
	}

}
