package com.taobao.top.analysis.node.component;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.Assert;


import org.junit.Test;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobMergedResult;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.statistics.StatisticsEngine;

/**
 * 运行时请清除掉output目录下的临时文件，防止干扰
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午3:41:33
 *
 */
public class JobResultMergerTest {

	@Test
	public void testMerge() throws AnalysisException, UnsupportedEncodingException, IOException, InterruptedException {
		JobResultMerger jobResultMerge = new JobResultMerger();
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		jobResultMerge.setConfig(config);
		jobResultMerge.init();
		
		BlockingQueue<JobMergedResult> branchResultQueue = new LinkedBlockingQueue<JobMergedResult>();
		BlockingQueue<JobTaskResult> jobTaskResultsQueue = new LinkedBlockingQueue<JobTaskResult>();
		
		
		StatisticsEngine defaultAnalysisEngine = new StatisticsEngine();
		defaultAnalysisEngine.init();
		
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		
		defaultAnalysisEngine.addInputAdaptor(fileInputAdaptor);
		defaultAnalysisEngine.addInputAdaptor(httpInputAdaptor);
		
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		mixJobBuilder.setConfig(config);
		mixJobBuilder.init();
		
		
		Map<String, Job> jobs = mixJobBuilder.build();
		Job job = jobs.values().iterator().next();
		List<JobTaskResult> mergeing = new ArrayList<JobTaskResult>();
		
		for(JobTask task : job.getJobTasks())
		{
			mergeing.add(defaultAnalysisEngine.doAnalysis(task));
			jobTaskResultsQueue.offer(defaultAnalysisEngine.doAnalysis(task));
		}
		
		jobResultMerge.merge(job, branchResultQueue, jobTaskResultsQueue, false);
		
		JobTaskResult mergedJobTask = jobResultMerge.merge(job.getJobTasks().get(0), mergeing, false);
		
		//多线程，需要休息一会儿
		Thread.sleep(2000);
		
		Map<String, Map<String, Object>> mergedResult = job.getJobResult();
		
		String key = mergedResult.keySet().iterator().next();
		String key2 = mergedResult.get(key).keySet().iterator().next();
		Object value = mergedResult.get(key).get(key2);
		
		Assert.assertEquals(mergedJobTask.getResults().get(key).get(key2), value);
			
		defaultAnalysisEngine.releaseResource();
		mixJobBuilder.releaseResource();		
		jobResultMerge.releaseResource();
	}

}
