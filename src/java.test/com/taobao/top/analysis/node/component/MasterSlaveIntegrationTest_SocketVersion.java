/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.io.File;
import java.io.IOException;

import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.connect.SocketMasterConnector;
import com.taobao.top.analysis.node.connect.SocketSlaveConnector;
import com.taobao.top.analysis.node.event.SlaveEventCode;
import com.taobao.top.analysis.node.event.SlaveNodeEvent;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.FileOutputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.io.IOutputAdaptor;
import com.taobao.top.analysis.statistics.StatisticsEngine;

/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-6 上午12:01:31
 *
 */
public class MasterSlaveIntegrationTest_SocketVersion {
	
	public static MasterNode buildMaster(String configfile) throws InterruptedException
	{
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
		config.load(configfile);
		masterNode.setConfig(config);
		masterNode.startNode();
		
		
		return masterNode;
	}
	
	public static SlaveNode buildSlave(String configfile,boolean needStart)
	{
		SlaveNode slaveNode = new SlaveNode();
		JobResultMerger jobResultMerger2 = new JobResultMerger();
		SocketSlaveConnector slaveConnector = new SocketSlaveConnector();
		slaveConnector.setDownstreamHandler(new ObjectEncoder());
		slaveConnector.setUpstreamHandler(new ObjectDecoder());
			
		StatisticsEngine statisticsEngine = new StatisticsEngine();
		SlaveConfig slaveConfig = new SlaveConfig();
		slaveConfig.load(configfile);
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
		
		if (needStart)
			slaveNode.startNode();
		
		return slaveNode;
	}
	
	@Test
	@Ignore
	public void test() throws AnalysisException, InterruptedException
	{
		//build MasterNode1
		MasterNode masterNode = buildMaster("master-config-ms.properties");
		MasterNode masterNode1 = buildMaster("master-config-ms1.properties");
		MasterNode masterNode2 = buildMaster("master-config-ms2.properties");
			
		
		//build SlaveNode
		SlaveNode slaveNode = buildSlave("slave-config.properties",false);
		SlaveNodeEvent event = new SlaveNodeEvent();
		event.setEventCode(SlaveEventCode.SUSPEND);
		slaveNode.addEvent(event);
		slaveNode.startNode();
		
		Thread.sleep(3000);
		
		event.setEventCode(SlaveEventCode.AWAKE);
		slaveNode.addEvent(event);
		
		
		Thread.sleep(35 * 1000);
		
		masterNode.stopNode();
		masterNode1.stopNode();
		masterNode2.stopNode();
		slaveNode.stopNode();
		
		Thread.sleep(3000);
		
	}
	
	@Test
	public void test1() throws AnalysisException, InterruptedException
	{
		//build MasterNode1
		MasterNode masterNode = buildMaster("master-config-ms.properties");
		MasterNode masterNode1 = buildMaster("master-config-ms1.properties");
		MasterNode masterNode2 = buildMaster("master-config-ms2.properties");
			
		Thread.sleep(1000);
		
		//build SlaveNode
		SlaveNode slaveNode = buildSlave("slave-config.properties",true);
		SlaveNode slaveNode1 = buildSlave("slave-config1.properties",true);
		
		Thread.sleep(35 * 1000);
		
		masterNode.stopNode();
		masterNode1.stopNode();
		masterNode2.stopNode();
		slaveNode.stopNode();
		slaveNode1.stopNode();
		
		Thread.sleep(3000);
		
	}
	
	@Test
	@Ignore
	public void testFailCover() throws AnalysisException, InterruptedException
	{
		//build MasterNode1
		MasterNode masterNode = buildMaster("master-config-ms.properties");
		MasterNode masterNode1 = buildMaster("master-config-ms1.properties");
		
		Thread.sleep(1000);
		
		
		//build SlaveNode
		SlaveNode slaveNode = buildSlave("slave-config.properties",true);
		
		
		Thread.sleep(35 * 1000);
		
		masterNode.stopNode();
		masterNode1.stopNode();
		
		File tmpFiles = new File("slave1/temp");
		
		
		MasterNode masterNode2 = buildMaster("master-config-ms2.properties");
		
		Thread.sleep(15 * 1000);
		
		masterNode2.stopNode();
		slaveNode.stopNode();
		
		Thread.sleep(3000);
		
	}

}
