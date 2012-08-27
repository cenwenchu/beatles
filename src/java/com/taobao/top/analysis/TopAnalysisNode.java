package com.taobao.top.analysis;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import com.taobao.top.analysis.config.AbstractConfig;
import com.taobao.top.analysis.config.IConfig;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobBuilder;
import com.taobao.top.analysis.node.IJobExporter;
import com.taobao.top.analysis.node.IJobManager;
import com.taobao.top.analysis.node.IJobResultMerger;
import com.taobao.top.analysis.node.INode;
import com.taobao.top.analysis.node.component.FileJobExporter;
import com.taobao.top.analysis.node.component.JobManager;
import com.taobao.top.analysis.node.component.JobResultMerger;
import com.taobao.top.analysis.node.component.MasterMonitor;
import com.taobao.top.analysis.node.component.MasterNode;
import com.taobao.top.analysis.node.component.MixJobBuilder;
import com.taobao.top.analysis.node.component.SlaveMonitor;
import com.taobao.top.analysis.node.component.SlaveNode;
import com.taobao.top.analysis.node.connect.IMasterConnector;
import com.taobao.top.analysis.node.connect.ISlaveConnector;
import com.taobao.top.analysis.node.connect.SocketMasterConnector;
import com.taobao.top.analysis.node.connect.SocketSlaveConnector;
import com.taobao.top.analysis.node.event.MasterEventCode;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.io.FileInputAdaptor;
import com.taobao.top.analysis.node.io.FileOutputAdaptor;
import com.taobao.top.analysis.node.io.HdfsInputAdaptor;
import com.taobao.top.analysis.node.io.HttpInputAdaptor;
import com.taobao.top.analysis.node.io.HubInputAdaptor;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.io.IOutputAdaptor;
import com.taobao.top.analysis.node.monitor.IMonitor;
import com.taobao.top.analysis.statistics.IStatisticsEngine;
import com.taobao.top.analysis.statistics.StatisticsEngine;
import com.taobao.top.analysis.util.AnalyzerUtil;

/**
 * TopAnalysisNode.java
 * @author yunzhan.jtq
 * 
 * @since 2012-2-3 上午10:40:15
 */
public class TopAnalysisNode implements Runnable {
    private static final Log log = LogFactory.getLog(TopAnalysisNode.class);
    
    /**
     * 如果是master，则初始化为MasterNode；
     * 如果是slave，则初始化为SlaveNode
     */
    private INode<?, ?> node = null;
    
    /**
     * 如果是master，则初始化为JobManager
     */
    private IJobManager jobManager = null;
    
    /**
     * 如果是master，则初始化为MixJobBuilder
     */
    private IJobBuilder jobBuilder = null;
    
    /**
     * 服务端和客户端交互通道
     * http访问
     */
//    private MemTunnel tunnel = null;
    
    /**
     * 服务端通信组件
     */
    private IMasterConnector masterConnector = null;
    
    /**
     * 客户端通信组件
     */
    private ISlaveConnector slaveConnector = null;
    
    /**
     * 监控组件
     */
    private IMonitor<?> monitor = null;
    
    /**
     * 任务合并组件
     */
    private IJobResultMerger jobResultMerger = null;
    
    /**
     * 报表输出组件
     * 如果是master，则初始化为FileJobExporter
     */
    private IJobExporter jobExporter = null;
    
    /**
     * 如果是master，则为masterConfig；
     * 如果是slave，则为slaveConfig
     */
    private IConfig nodeConfig = null;
    
    /**
     * 计算引擎
     */
    private IStatisticsEngine statisticsEngine = null;
    
    /**
     * http输入适配器
     */
    private IInputAdaptor httpInputAdaptor = null;
    
    /**
     * hdfs输入适配器
     */
    private IInputAdaptor hdfsInputAdaptor = null;
    
    /**
     * file输入适配器
     */
    private IInputAdaptor fileInputAdaptor = null;
    
    /**
     * hub输入适配器
     */
    private IInputAdaptor hubInputAdaptor = null;
    
    /**
     * file输出适配器
     */
    private IOutputAdaptor fileOutputAdaptor = null;
    
    /**
     * 初始化标志
     */
    private AtomicBoolean init = new AtomicBoolean(false);
    
    /**
     * 同步http服务
     */
    private HttpAgentNode httpAgentNode = null;
    
    /**
     * 扫描线程
     * 目前只用于扫描配置文件变更
     */
    private java.util.concurrent.ScheduledExecutorService scanService = java.util.concurrent.Executors
    .newScheduledThreadPool(3);

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args == null || args.length < 2 || args[0] == null
                || args[0].equals("") || args[1] == null || args[1].equals("")) {
            System.out.println("usage : java -jar TopAnalyzer.jar propFile master");
            log.error("usage : java -jar TopAnalyzer.jar propFile master");
            return;
        }

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                 if (!(e instanceof ThreadDeath)) {
                        System.err.print("Exception in thread \""
                                 + t.getName() + "\" ");
                                e.printStackTrace(System.err);
            }   
            }
        });
        
        System.out.println("BuildDate:"+AnalyzerUtil.getManifestBuildDate());
        
        final TopAnalysisNode topAnalyzerNode = new TopAnalysisNode();
        
        try {
            topAnalyzerNode.init(args[0], "master".equalsIgnoreCase(args[1]));
        } catch(Throwable e) {
            log.error(e, e);
        }
        topAnalyzerNode.start();
//        topAnalyzerNode.run();
    }
    
    /**
     * 初始化各个类
     * 很多配置目前是硬编码，后续将根据配置的不同选择不同的配置
     */
    public void init(String propertyFile, boolean isMaster) {
        jobResultMerger = new JobResultMerger();
        jobExporter = new FileJobExporter();
        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
        if(isMaster) {
            //init Master
            buildMaster();
        } else {
            //init slave
            buildSlave();
        }
        
        nodeConfig.load(propertyFile);
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
            @Override
            public void run() {
                releaseResource();
            }
            
        }));
        this.init.compareAndSet(false, true);
    }
    
    /**
     * 开启节点
     * 并开启配置扫描线程
     */
    public void start() {
        if(!this.init.get()) {
            log.error("node init failed, please check the config");
            throw new java.lang.RuntimeException("node init failed, please check the config");
        }
        node.startNode();
        httpAgentNode.start();
        scanService.scheduleAtFixedRate(this, 5, ((AbstractConfig)nodeConfig).getScanFileTime(), TimeUnit.SECONDS);
    }
    
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        try {
            this.checkConfigModified();
        } catch (Throwable e) {
            log.error(e);
        }
    }
    
    
    /**
     * 初始化master
     */
    private void buildMaster() {
        node = new MasterNode();
        nodeConfig = new MasterConfig();
        jobManager = new JobManager();
        jobBuilder = new MixJobBuilder();
        masterConnector = new SocketMasterConnector();
        monitor = new MasterMonitor();
        httpAgentNode = new HttpAgentNode();
        
        ((MasterNode)node).setConfig((MasterConfig)nodeConfig);
        
        ObjectDecoder objDecoder = new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader()));
        ((SocketMasterConnector)masterConnector).setDownstreamHandler(new ObjectEncoder());
        ((SocketMasterConnector)masterConnector).setUpstreamHandler(objDecoder);
        
        jobManager.setJobBuilder(jobBuilder);
        jobManager.setJobExporter(jobExporter);
        jobManager.setJobResultMerger(jobResultMerger);
        
        ((MasterNode)node).setJobManager(jobManager);
        ((MasterNode)node).setMasterConnector(masterConnector);
        ((MasterNode)node).setMonitor((MasterMonitor)monitor);
        httpAgentNode.setJobManager(jobManager);
        httpAgentNode.setName("top-analysis-http-agent");
    }
    
    /**
     * 初始化slave
     */
    private void buildSlave() {
        node = new SlaveNode();
        nodeConfig = new SlaveConfig();
        slaveConnector = new SocketSlaveConnector();
        monitor = new SlaveMonitor();
        statisticsEngine = new StatisticsEngine();
        httpInputAdaptor = new HttpInputAdaptor();
        hdfsInputAdaptor = new HdfsInputAdaptor();
        fileInputAdaptor = new FileInputAdaptor();
        hubInputAdaptor = new HubInputAdaptor();
        fileOutputAdaptor = new FileOutputAdaptor();
        
        try {
            jobExporter.init();
        }
        catch (AnalysisException e) {
            log.error("init jobExporter failed", e);
        }
        ((FileOutputAdaptor)fileOutputAdaptor).setJobExporter(jobExporter);
        
        ((SocketSlaveConnector)slaveConnector).setDownstreamHandler(new ObjectEncoder(8192 * 4));
        ((SocketSlaveConnector)slaveConnector).setUpstreamHandler(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())));
        
        ((SlaveNode)node).setConfig((SlaveConfig) nodeConfig);
        ((SlaveNode)node).setJobResultMerger(jobResultMerger);
        ((SlaveNode)node).setSlaveConnector(slaveConnector);
        ((SlaveNode)node).setStatisticsEngine(statisticsEngine);
        ((SlaveNode)node).setMonitor((SlaveMonitor)monitor);
        
        statisticsEngine.addInputAdaptor(httpInputAdaptor);
        statisticsEngine.addInputAdaptor(hdfsInputAdaptor);
        statisticsEngine.addInputAdaptor(fileInputAdaptor);
        statisticsEngine.addInputAdaptor(hubInputAdaptor);
        statisticsEngine.addOutputAdaptor(fileOutputAdaptor);
    }
    
    private void checkConfigModified() {
        if(!this.init.get())
            return;
        if(nodeConfig.isNeedReload()) {
            nodeConfig.reload();
            log.error("node'config is modified, reloading executed, please have a check"); 
        }
        if(!(nodeConfig instanceof MasterConfig) || jobBuilder == null)
            return;
        if (jobBuilder.isModified()) {
            MasterNodeEvent e = new MasterNodeEvent();
            e.setEventCode(MasterEventCode.RELOAD_JOBS);
            ((MasterNode)node).addEvent(e);
            log.error("job'config is modified, reloading executed, please have a check");
            String content = null;
            try {
                content = "Host:"
                    + InetAddress.getLocalHost().getHostAddress()
                    + ",config file was modified!!!";
            }
            catch (UnknownHostException e1) {
                log.error(e1);
            }
            AbstractConfig tmpConfig = (AbstractConfig)nodeConfig;
            if(tmpConfig.isEnableAlert())
                AnalyzerUtil.sendOutAlert(java.util.Calendar.getInstance(), tmpConfig.getAlertUrl(), tmpConfig.getAlertFrom(), tmpConfig.getAlertModel(), tmpConfig.getAlertWangWang(), content);
        }
    }

    public void releaseResource() {
        node.releaseResource();
        scanService.shutdown();
    }
    
    public void stopNode() {
        node.stopNode();
    }
}
