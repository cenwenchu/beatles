/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.statistics.StatisticsEngine;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午1:13:56
 *
 */
public class FileJobExporterTest {

	
	@Test
	public void testExportReportJob() throws AnalysisException, UnsupportedEncodingException, IOException {
		StatisticsEngine defaultAnalysisEngine = new StatisticsEngine();
		defaultAnalysisEngine.init();
		
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		
		defaultAnalysisEngine.addInputAdaptor(fileInputAdaptor);
		defaultAnalysisEngine.addInputAdaptor(httpInputAdaptor);
		
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		FileJobExporter fileJobExporter = new FileJobExporter();
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		fileJobExporter.setConfig(config);
		mixJobBuilder.setConfig(config);
		mixJobBuilder.init();
		fileJobExporter.init();
		
		Map<String, Job> jobs = mixJobBuilder.build();
		
		for(Job job : jobs.values())
		{
			JobTask task = job.getJobTasks().get(0);
			
			job.setJobResult(defaultAnalysisEngine.doAnalysis(task).getResults());	
			
			fileJobExporter.exportReport(job, false);
		}
			
		defaultAnalysisEngine.releaseResource();
		mixJobBuilder.releaseResource();
		fileJobExporter.releaseResource();
	}

	
	@Test
	public void testExportReportJobTaskJobTaskResult() throws AnalysisException, UnsupportedEncodingException, IOException {
		StatisticsEngine defaultAnalysisEngine = new StatisticsEngine();
		defaultAnalysisEngine.init();
		
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		
		defaultAnalysisEngine.addInputAdaptor(fileInputAdaptor);
		defaultAnalysisEngine.addInputAdaptor(httpInputAdaptor);
		
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		FileJobExporter fileJobExporter = new FileJobExporter();
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		fileJobExporter.setConfig(config);
		mixJobBuilder.setConfig(config);
		mixJobBuilder.init();
		fileJobExporter.init();
		
		Map<String, Job> jobs = mixJobBuilder.build();
		
		for(Job job : jobs.values())
		{
			JobTask task = job.getJobTasks().get(0);
			
			JobTaskResult jobTaskResult = defaultAnalysisEngine.doAnalysis(task);	
			
			fileJobExporter.exportReport(task, jobTaskResult, false);
		}
			
		defaultAnalysisEngine.releaseResource();
		mixJobBuilder.releaseResource();
		fileJobExporter.releaseResource();
	}

	
	@Test
	public void testExportEntryDataAndLoadEntryData() throws AnalysisException, UnsupportedEncodingException, IOException, InterruptedException {
		
		StatisticsEngine defaultAnalysisEngine = new StatisticsEngine();
		defaultAnalysisEngine.init();
		
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		
		defaultAnalysisEngine.addInputAdaptor(fileInputAdaptor);
		defaultAnalysisEngine.addInputAdaptor(httpInputAdaptor);
		
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		FileJobExporter fileJobExporter = new FileJobExporter();
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		fileJobExporter.setConfig(config);
		mixJobBuilder.setConfig(config);
		mixJobBuilder.init();
		fileJobExporter.init();
		
		Map<String, Job> jobs = mixJobBuilder.build();
		Job job = jobs.values().iterator().next();
		
		JobTask task = job.getJobTasks().get(0);
		
		job.setJobResult(defaultAnalysisEngine.doAnalysis(task).getResults());
		
		fileJobExporter.exportEntryData(job);
		
		Thread.sleep(1000);
		
		Map<String, Map<String, Object>> result = job.getJobResult();
		job.setJobResult(null);
		
		fileJobExporter.loadEntryData(job);
		fileJobExporter.loadEntryDataToTmp(job);
		
		Thread.sleep(1000);
		
		String key = result.keySet().iterator().next();
		String key2 = result.get(key).keySet().iterator().next();
		Object value = result.get(key).get(key2);
		
		Assert.assertEquals(job.getJobResult().get(key).get(key2), value);
		Assert.assertEquals(job.getDiskResult().get(key).get(key2), value);
		
			
		defaultAnalysisEngine.releaseResource();
		mixJobBuilder.releaseResource();
		fileJobExporter.releaseResource();
		
	}

}
