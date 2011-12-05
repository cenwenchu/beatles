/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.util.TimeOutQueue;

/**
 * Socket版本的客户端通信组件
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-2 下午5:26:51
 *
 */
public class SocketSlaveConnector extends AbstractSlaveConnector {

	private static final Log logger = LogFactory.getLog(SocketSlaveConnector.class);
	
	ClientBootstrap bootstrap;
	ChannelFactory factory;
	ChannelFuture future;
	Channel channel;
	ChannelDownstreamHandler  downstreamHandler;
	ChannelUpstreamHandler upstreamHandler;
	Map<String,MasterNodeEvent> responseQueue = new ConcurrentHashMap<String,MasterNodeEvent>();
	SlaveEventTimeOutQueue slaveEventTimeQueue;
	
	@Override
	public void init() throws AnalysisException {
		 factory = new NioClientSocketChannelFactory  (
	                    Executors.newCachedThreadPool(),
	                    Executors.newCachedThreadPool());
		 
	     bootstrap = new ClientBootstrap(factory);
	     
	     bootstrap.setPipelineFactory(
	    	 new ChannelPipelineFactory() 
	     	 {
	    		 public ChannelPipeline getPipeline() 
	    		 {
	                return Channels.pipeline(downstreamHandler,upstreamHandler,new SlaveConnectorHandler(responseQueue,slaveEventTimeQueue));
	    		 }
	     	 });
	     
	     bootstrap.setOption("tcpNoDelay"  , true);
	     bootstrap.setOption("keepAlive", true);
	        
	     connectServer();
	     
	     slaveEventTimeQueue = new SlaveEventTimeOutQueue();
	}
	
	public void connectServer()
	{
		if (channel != null)
		{
			releaseResource();
		}
		
		future = bootstrap.connect(new InetSocketAddress(config.getMasterAddress(), config.getMasterPort()));
	     
	     future.awaitUninterruptibly();
	     if (!future.isSuccess()) {
           logger.error(future.getCause());
	     }
	     
	     channel = future.getChannel(); 
	}

	@Override
	public void releaseResource() {
		try
		{
			if (slaveEventTimeQueue != null)
				slaveEventTimeQueue.release();
			
			channel.getCloseFuture().awaitUninterruptibly();
			factory.releaseExternalResources();
			responseQueue.clear();
		}
		catch(Exception ex)
		{
			logger.error(ex,ex);
		}
        
        logger.info("SocketSlaveConnector releaseResource now.");
	}
	
	@Override
	public JobTask[] getJobTasks(GetTaskRequestEvent requestEvent) {
		
		JobTask[] tasks = null;
		
		try
		{
			ChannelFuture channelFuture = channel.write(requestEvent);
		
			channelFuture.await(10, TimeUnit.SECONDS);
			
			channelFuture.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
			
			//简单的用这种模式模拟阻塞请求
			responseQueue.put(requestEvent.getSequence(), requestEvent);
			slaveEventTimeQueue.add(requestEvent);
			
			requestEvent.getResultReadyFlag().await(config.getMaxClientEventWaitTime(), TimeUnit.SECONDS);
			
			GetTaskResponseEvent responseEvent = (GetTaskResponseEvent)requestEvent.getResponse();
			
			if (responseEvent != null && responseEvent.getJobTasks() != null && responseEvent.getJobTasks().size() > 0)
			{
				tasks = new JobTask[responseEvent.getJobTasks().size()];
				responseEvent.getJobTasks().toArray(tasks);
			}
			
		}
		catch(Exception ex)
		{
			logger.error(ex,ex);
		}
		
		return tasks;
	}

	
	@Override
	public String sendJobTaskResults(SendResultsRequestEvent jobResponseEvent) {
		
		try
		{
			ChannelFuture channelFuture = channel.write(jobResponseEvent);
			channelFuture.await(10, TimeUnit.SECONDS);
			
			channelFuture.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
			
			//简单的用这种模式模拟阻塞请求
			responseQueue.put(jobResponseEvent.getSequence(), jobResponseEvent);
			slaveEventTimeQueue.add(jobResponseEvent);
			
			jobResponseEvent.getResultReadyFlag().await(config.getMaxClientEventWaitTime(), TimeUnit.SECONDS);
			
			SendResultsResponseEvent responseEvent = (SendResultsResponseEvent)jobResponseEvent.getResponse();
			
			if (responseEvent != null)
			{
				return responseEvent.getResponse();
			}
		}
		catch(Exception ex)
		{
			logger.equals(ex);
		}
		
		return null;
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
	
	class SlaveEventTimeOutQueue extends TimeOutQueue<MasterNodeEvent>
	{
		@Override
		public void timeOutAction(MasterNodeEvent event) {
			if (responseQueue.containsKey(event.getSequence()))
				responseQueue.remove(event.getSequence());
		}
	}

}
