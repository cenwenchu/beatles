/**
 * 
 */
package com.taobao.top.analysis.node.component;

import org.junit.Test;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.connect.MemMasterConnector;
import com.taobao.top.analysis.node.connect.MemSlaveConnector;
import com.taobao.top.analysis.node.connect.MemTunnel;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.FileOutputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.io.IOutputAdaptor;
import com.taobao.top.analysis.statistics.StatisticsEngine;

/**
 * 集成MasterSlave测试
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午5:06:21
 *
 */
public class MasterSlaveIntegrationTest {
	
	@Test
	public void test() throws AnalysisException, InterruptedException
	{
		//build MasterNode
		MasterNode masterNode = new MasterNode();
		MemMasterConnector masterConnector = new MemMasterConnector();
		JobManager jobManager = new JobManager();
		JobResultMerger jobResultMerger = new JobResultMerger();
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		FileJobExporter fileJobExporter = new FileJobExporter();
		MemTunnel tunnel = new MemTunnel();
		
		jobManager.setJobBuilder(mixJobBuilder);
		jobManager.setJobExporter(fileJobExporter);
		jobManager.setJobResultMerger(jobResultMerger);
		masterConnector.setTunnel(tunnel);
		masterNode.setJobManager(jobManager);
		masterNode.setMasterConnector(masterConnector);	
		
		MasterConfig config = new MasterConfig();
		config.load("master-config-ms.properties");
		masterNode.setConfig(config);
		masterNode.startNode();
		
		
		//build SlaveNode
		SlaveNode slaveNode = new SlaveNode();
		JobResultMerger jobResultMerger2 = new JobResultMerger();
		MemSlaveConnector slaveConnector = new MemSlaveConnector();
		slaveConnector.setTunnel(tunnel);
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

		
		FileJobExporter fileJobExporter2 = new FileJobExporter();
		fileJobExporter2.setMaxCreateReportWorker(2);
		fileJobExporter2.init();
		
		((FileOutputAdaptor)fileOutAdaptor).setJobExporter(fileJobExporter2);
		
		statisticsEngine.addInputAdaptor(fileInputAdaptor);
		statisticsEngine.addInputAdaptor(httpInputAdaptor);
		statisticsEngine.addOutputAdaptor(fileOutAdaptor);
		slaveNode.startNode();
		
		
		Thread.sleep(30 * 1000);
		
		masterNode.stopNode();
		slaveNode.stopNode();
		
		Thread.sleep(3000);
		
	}

}
