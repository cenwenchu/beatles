package com.taobao.top.analysis.node.component;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.connect.MemMasterConnector;
import com.taobao.top.analysis.node.connect.MemTunnel;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.statistics.StatisticsEngine;

public class JobManagerTest {

	MasterNode masterNode;
	JobManager jobManager;
	MixJobBuilder mixJobBuilder;
	MemTunnel tunnel;
	
	
	@Before
	public void setUp() throws Exception {
		masterNode = new MasterNode();
		MemMasterConnector masterConnector = new MemMasterConnector();
		jobManager = new JobManager();
		JobResultMerger jobResultMerger = new JobResultMerger();
		mixJobBuilder = new MixJobBuilder();
		FileJobExporter fileJobExporter = new FileJobExporter();
		tunnel = new MemTunnel();
		
		jobManager.setJobBuilder(mixJobBuilder);
		jobManager.setJobExporter(fileJobExporter);
		jobManager.setJobResultMerger(jobResultMerger);
		masterConnector.setTunnel(tunnel);
		masterNode.setJobManager(jobManager);
		masterNode.setMasterConnector(masterConnector);	
		
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		masterNode.setConfig(config);
		masterNode.init();
	}

	@After
	public void tearDown() throws Exception {
		masterNode.releaseResource();
	}

	@Test
	public void testGetUnDoJobTasks() throws AnalysisException {
		
		
		GetTaskRequestEvent event = new GetTaskRequestEvent("1234567");
		event.setRequestJobCount(2);
		
		jobManager.getUnDoJobTasks(event);
		
		GetTaskResponseEvent eventresp = (GetTaskResponseEvent)tunnel.getSlaveSide().poll();
		
		Assert.assertEquals(event.getSequence(), eventresp.getSequence());
		Assert.assertEquals(2, eventresp.getJobTasks().size());
		
		event = new GetTaskRequestEvent("1234567");
		event.setRequestJobCount(2);
		event.setJobName("job2");
		
		jobManager.getUnDoJobTasks(event);
		
		eventresp = (GetTaskResponseEvent)tunnel.getSlaveSide().poll();
		
		Assert.assertEquals(event.getSequence(), eventresp.getSequence());
		Assert.assertEquals(1, eventresp.getJobTasks().size());
		
	}

	@Test
	public void testAddTaskResultToQueue() throws AnalysisException, UnsupportedEncodingException, IOException {
		StatisticsEngine defaultAnalysisEngine = new StatisticsEngine();
		defaultAnalysisEngine.init();
		
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		
		defaultAnalysisEngine.addInputAdaptor(fileInputAdaptor);
		defaultAnalysisEngine.addInputAdaptor(httpInputAdaptor);
		
		SendResultsRequestEvent jobResponseEvent = new SendResultsRequestEvent("1234");
	
		JobTask task = jobManager.getJobs().values().iterator().next().getJobTasks().get(0);
		
		JobTaskResult jobTaskResult = defaultAnalysisEngine.doAnalysis(task);	
	
		jobResponseEvent.setJobTaskResult(jobTaskResult);
		
		jobManager.addTaskResultToQueue(jobResponseEvent);
		
		
		JobTaskResult jobTaskResult2 = jobManager.getJobTaskResultsQueuePool().get(task.getJobName()).poll();
		
		Assert.assertEquals(jobTaskResult, jobTaskResult2);
		
		SendResultsResponseEvent sendResultsResponseEvent = (SendResultsResponseEvent)tunnel.getSlaveSide().poll();
		
		Assert.assertEquals("success", sendResultsResponseEvent.getResponse());
		
		
		//验证少了一个任务
		GetTaskRequestEvent event = new GetTaskRequestEvent("1234567");
		event.setRequestJobCount(3);
		
		jobManager.getUnDoJobTasks(event);
		
		GetTaskResponseEvent eventresp = (GetTaskResponseEvent)tunnel.getSlaveSide().poll();
		
		Assert.assertEquals(event.getSequence(), eventresp.getSequence());
		Assert.assertEquals(2, eventresp.getJobTasks().size());
		
		defaultAnalysisEngine.releaseResource();
	}

	@Test
	public void testCheckJobStatus() throws InterruptedException, AnalysisException {
		//验证一共3个任务
		GetTaskRequestEvent event = new GetTaskRequestEvent("1234567");
		event.setRequestJobCount(3);
		
		jobManager.getUnDoJobTasks(event);
		
		GetTaskResponseEvent eventresp = (GetTaskResponseEvent)tunnel.getSlaveSide().poll();
		
		Assert.assertEquals(event.getSequence(), eventresp.getSequence());
		Assert.assertEquals(3, eventresp.getJobTasks().size());
		
		//验证没有任务
		event = new GetTaskRequestEvent("12345678");
		event.setRequestJobCount(3);
		
		jobManager.getUnDoJobTasks(event);
		
		eventresp = (GetTaskResponseEvent)tunnel.getSlaveSide().poll();
		
		Assert.assertEquals(event.getSequence(), eventresp.getSequence());
		Assert.assertEquals(0, eventresp.getJobTasks().size());
		
		//任务被回收
		Thread.sleep(21 * 1000);
		
		//验证一共3个任务
		jobManager.checkJobStatus();
		
		event = new GetTaskRequestEvent("12345679");
		event.setRequestJobCount(3);
		
		jobManager.getUnDoJobTasks(event);
		
		eventresp = (GetTaskResponseEvent)tunnel.getSlaveSide().poll();
		
		Assert.assertEquals(event.getSequence(), eventresp.getSequence());
		Assert.assertEquals(3, eventresp.getJobTasks().size());
		
		Assert.assertEquals(1,eventresp.getJobTasks().get(0).getRecycleCounter().get());
				
	}

}
