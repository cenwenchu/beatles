/**
 * 
 */
package com.taobao.top.analysis.node.connect;


import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;

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

	@Override
	public void init() throws AnalysisException 
	{
		bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
		
	    bootstrap.setPipelineFactory(
	    		new ChannelPipelineFactory() 
	    		{
	    			public ChannelPipeline getPipeline() 
	    			{
	    				return Channels.pipeline(downstreamHandler,upstreamHandler,new MasterConnectorHandler(masterNode));
	            	}
	        	});
	    
	    bootstrap.setOption("child.tcpNoDelay", true);
	    bootstrap.setOption("child.keepAlive", true);
	    bootstrap.setOption("child.receiveBufferSize", 9753);
        bootstrap.setOption("child.sendBufferSize", 8642);
        serverChannel = bootstrap.bind(new InetSocketAddress(config.getMasterPort()));
	    
	    logger.info("SocketMasterConnector init now.");
	}
	
	public void openServer()
	{
		if (serverChannel != null)
		{
			releaseResource();
		}
		
		serverChannel = bootstrap.bind(new InetSocketAddress(config.getMasterPort()));
	}

	@Override
	public void releaseResource() {
		
		try
		{
			serverChannel.close().awaitUninterruptibly();
			bootstrap.getFactory().releaseExternalResources();
		}
		catch(Exception ex)
		{
			logger.error(ex,ex);
		}
        
        logger.info("SocketMasterConnector releaseResource now.");
	}
	
	@Override
	public void echoGetJobTasks(GetTaskResponseEvent event) {
		ChannelFuture channelFuture = ((Channel)event.getChannel()).write(event);
		
		try {
			channelFuture.await(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		
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
	public void echoSendJobTaskResults(SendResultsResponseEvent event) {
		ChannelFuture channelFuture = ((Channel)event.getChannel()).write(event);
		
		try {
			channelFuture.await(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		
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
