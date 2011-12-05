/**
 * 
 */
package com.taobao.top.analysis.node.component;

import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.junit.Test;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.connect.SocketMasterConnector;
import com.taobao.top.analysis.node.connect.SocketSlaveConnector;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.FileOutputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.io.IOutputAdaptor;
import com.taobao.top.analysis.node.io.MasterOutputAdaptor;
import com.taobao.top.analysis.statistics.StatisticsEngine;

/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-6 上午12:01:31
 *
 */
public class MasterSlaveIntegrationTest_SocketVersion {
	
	@Test
	public void test() throws AnalysisException, InterruptedException
	{
		//build MasterNode
		MasterNode masterNode = new MasterNode();
		SocketMasterConnector masterConnector = new SocketMasterConnector();
		JobManager jobManager = new JobManager();
		JobResultMerger jobResultMerger = new JobResultMerger();
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		FileJobExporter fileJobExporter = new FileJobExporter();
		
		
		jobManager.setJobBuilder(mixJobBuilder);
		jobManager.setJobExporter(fileJobExporter);
		jobManager.setJobResultMerger(jobResultMerger);
		
		masterConnector.setDownstreamHandler(new ObjectEncoder());
		masterConnector.setUpstreamHandler(new ObjectDecoder());
		
		masterNode.setJobManager(jobManager);
		masterNode.setMasterConnector(masterConnector);	
		
		MasterConfig config = new MasterConfig();
		config.load("master-config-ms.properties");
		masterNode.setConfig(config);
		masterNode.startNode();
		
		Thread.sleep(1000);
		
		//build SlaveNode
		SlaveNode slaveNode = new SlaveNode();
		JobResultMerger jobResultMerger2 = new JobResultMerger();
		SocketSlaveConnector slaveConnector = new SocketSlaveConnector();
		slaveConnector.setDownstreamHandler(new ObjectEncoder());
		slaveConnector.setUpstreamHandler(new ObjectDecoder());
			
		StatisticsEngine statisticsEngine = new StatisticsEngine();
		SlaveConfig slaveConfig = new SlaveConfig();
		slaveConfig.load("slave-config.properties");
		slaveNode.setConfig(slaveConfig);
		slaveNode.setSlaveConnector(slaveConnector);
		slaveNode.setStatisticsEngine(statisticsEngine);
		slaveNode.setJobResultMerger(jobResultMerger2);
		
		IInputAdaptor fileInputAdaptor =  new FileInputAdaptor();
		IInputAdaptor httpInputAdaptor = new HttpInputAdaptor();
		IOutputAdaptor fileOutAdaptor = new FileOutputAdaptor();
		IOutputAdaptor masterOutputAdaptor = new MasterOutputAdaptor();
		((MasterOutputAdaptor)masterOutputAdaptor).setSlaveConnector(slaveConnector);
		
		FileJobExporter fileJobExporter2 = new FileJobExporter();
		fileJobExporter2.setMaxCreateReportWorker(2);
		fileJobExporter2.init();
		
		((FileOutputAdaptor)fileOutAdaptor).setJobExporter(fileJobExporter2);
		
		statisticsEngine.addInputAdaptor(fileInputAdaptor);
		statisticsEngine.addInputAdaptor(httpInputAdaptor);
		statisticsEngine.addOutputAdaptor(fileOutAdaptor);
		statisticsEngine.addOutputAdaptor(masterOutputAdaptor);
		slaveNode.startNode();
		
		
		Thread.sleep(30 * 1000);
		
		masterNode.stopNode();
		slaveNode.stopNode();
		
		Thread.sleep(3000);
		
	}

}
