/**
 * 
 */
package com.taobao.top.analysis.node.connect;


import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.SendMonitorInfoResponseEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;
import com.taobao.top.analysis.node.job.JobTask;

/**
 * Socket版本的服务端通信组件
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-2 下午5:15:22
 *
 */
public class SocketMasterConnector extends AbstractMasterConnector{

	private static final Log logger = LogFactory.getLog(SocketMasterConnector.class);
	
	ServerBootstrap bootstrap;
	Channel serverChannel;
	ChannelDownstreamHandler  downstreamHandler;
	ChannelUpstreamHandler upstreamHandler;
	private static final ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool());

	@Override
	public void init() throws AnalysisException 
 {
	    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
        bootstrap =
                new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline pipe =
                        Channels.pipeline(
                            new ObjectEncoder(),
                            new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.weakCachingConcurrentResolver(this
                                .getClass().getClassLoader())), new MasterConnectorHandler(masterNode));
                pipe.addLast("log", new LoggingHandler());
                return pipe;
            }
        });
        
        bootstrap.setOption("reuseAddress", true);

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.receiveBufferSize", 8192 * 4); // 9753
        bootstrap.setOption("child.sendBufferSize", 4096); // 8642
        bootstrap.setOption("child.connectTimeoutMillis", 10000);
        serverChannel = bootstrap.bind(new InetSocketAddress(config.getMasterPort()));

        logger.info("SocketMasterConnector init now.");
    }
	
	public void openServer()
	{
		if (serverChannel != null)
		{
			releaseResource();
		}
		
		try {
            init();
        }
        catch (AnalysisException e) {
            logger.error("reinit server error", e);
        }
	}

	@Override
	public void releaseResource() {
		
		try
		{
			serverChannel.close().awaitUninterruptibly();
			bootstrap.getFactory().releaseExternalResources();
			bootstrap.releaseExternalResources();
		}
		catch(Exception ex)
		{
			logger.error(ex,ex);
		}
        
        logger.info("SocketMasterConnector releaseResource now.");
	}
	
	@Override
	public void echoGetJobTasks(final GetTaskResponseEvent event) {
		ChannelFuture channelFuture = ((Channel)event.getChannel()).write(event);
		
//		这里的线程等待只是为了阻塞线程？
//		try {
//			channelFuture.await(10, TimeUnit.SECONDS);
//		} catch (InterruptedException e) {
//		}
		
		channelFuture.addListener(new ChannelFutureListener() {
	        public void operationComplete(ChannelFuture future) {
	            if(future.isSuccess()) {
	                //warn的日志，打点为了对每轮执行情况进行观察
	                if (logger.isWarnEnabled())
	                {
	                    List<JobTask> jobTasks = event.getJobTasks();
	                    if (jobTasks != null && jobTasks.size() > 0)
	                    {
	                        StringBuilder sb = new StringBuilder("Send " + jobTasks.size() + " tasks to slave(" + future.getChannel().getRemoteAddress() + "), tasks : { ");
	                        
	                        for(JobTask t : jobTasks)
	                        {
	                            sb.append("taskId:").append(t.getTaskId()).append(",");
	                            sb.append("taskInput:").append(t.getInput()).append(";");
	                        }
	                        sb.append(" }");
	                        logger.warn(sb.toString());
	                        
	                    }
	                }
	            }
	            if (!future.isSuccess()) 
	            {
	            	logger.error("Mastersocket write error.",future.getCause());
	                future.getChannel().close();
	            }
	        }
	    });
		
	}

	@Override
	public void echoSendJobTaskResults(SendResultsResponseEvent event) {
		ChannelFuture channelFuture = ((Channel)event.getChannel()).write(event);
		
//		try {
//			channelFuture.await(10, TimeUnit.SECONDS);
//		} catch (InterruptedException e) {
//		}
		
		channelFuture.addListener(new ChannelFutureListener() {
	        public void operationComplete(ChannelFuture future) {
	            if (!future.isSuccess()) 
	            {
	            	logger.error("Mastersocket write error.",future.getCause());
	                future.getChannel().close();
	            }
	        }
	    });
	}
	
	@Override
	public void echoSendMonitorInfo(SendMonitorInfoResponseEvent event) {
		
		ChannelFuture channelFuture = ((Channel)event.getChannel()).write(event);
		channelFuture.addListener(new ChannelFutureListener() {
	        public void operationComplete(ChannelFuture future) {
	            if (!future.isSuccess()) 
	            {
	            	logger.error("Mastersocket write error.",future.getCause());
	                future.getChannel().close();
	            }
	        }
	    });
	}

	public ChannelDownstreamHandler getDownstreamHandler() {
		return downstreamHandler;
	}

	public void setDownstreamHandler(ChannelDownstreamHandler downstreamHandler) {
		this.downstreamHandler = downstreamHandler;
	}

	public ChannelUpstreamHandler getUpstreamHandler() {
		return upstreamHandler;
	}

	public void setUpstreamHandler(ChannelUpstreamHandler upstreamHandler) {
		this.upstreamHandler = upstreamHandler;
	}

}
