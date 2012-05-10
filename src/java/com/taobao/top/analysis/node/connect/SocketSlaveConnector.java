/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
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
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.logging.LoggingHandler;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;
import com.taobao.top.analysis.node.job.JobTask;

/**
 * Socket版本的客户端通信组件
 * 
 * @author fangweng
 * @author yunzhan.jtq
 * @email: fangweng@taobao.com 2011-12-2 下午5:26:51
 * 
 */
public class SocketSlaveConnector extends AbstractSlaveConnector {

	private static final Log logger = LogFactory
			.getLog(SocketSlaveConnector.class);

	ClientBootstrap bootstrap;
	private static final ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
	ChannelFuture future;
	//默认的channel
	String leaderChannel;
	ChannelDownstreamHandler downstreamHandler;
	ChannelUpstreamHandler upstreamHandler;
	Map<String, MasterNodeEvent> responseQueue = new ConcurrentHashMap<String, MasterNodeEvent>();
//	SlaveEventTimeOutQueue slaveEventTimeQueue;
	
	//支持多个master来分担合并压力
	Map<String,Channel> channels;
	//用于创建管道时候控制并发
	ReentrantLock channelLock;
	
	@Override
    public void init() throws AnalysisException {
//        slaveEventTimeQueue = new SlaveEventTimeOutQueue();
        channelLock = new ReentrantLock();

        bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline pipe =
                        Channels.pipeline(new ObjectEncoder(8192 * 4), new ObjectDecoder(Integer.MAX_VALUE,
                            ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())),
                            new SlaveConnectorHandler(responseQueue, slaveNode));
                pipe.addLast("log", new LoggingHandler());
                return pipe;
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("connectTimeoutMillis", 10000);

        initChannelPool();
        if (logger.isInfoEnabled()) {
            logger.info("init slave, trying to connect " + leaderChannel);
        }

    }

	public void initChannelPool() throws AnalysisException {
		if (channels != null) {
			releaseResource();
		}
		
		channels = new HashMap<String,Channel>();
		
		leaderChannel = new StringBuilder().append(config.getMasterAddress())
					.append(":").append(config.getMasterPort()).toString();
	}
	
	public Channel getChannel(String address) throws AnalysisException
	{
		Channel channel = channels.get(address);
		
		//目前简单实现连接失败重连，后续可考虑在此处实现连接失败的策略
		if (channel != null && (channel.isConnected() || channel.isOpen())) {
			return channel;
		}
		
		String[] _addr = StringUtils.split(address,":");
		boolean isLock = false;
			
		try
		{
			isLock = channelLock.tryLock(10, TimeUnit.SECONDS);
			
			if (isLock)
			{
				//double check
				channel = channels.get(address);
				
				if (channel != null && (channel.isConnected() || channel.isOpen()))
					return channel;
				
				logger.info("trying to open new channel");
				
				future = bootstrap.connect(new InetSocketAddress(_addr[0],Integer.valueOf(_addr[1])));
				logger.info("open channel to " + address );
	
				future.awaitUninterruptibly();
				if (!future.isSuccess()) {
					logger.error("connect fail.", future.getCause());
					throw new AnalysisException("connect fail.", future.getCause());
				}
	
				channel = future.getChannel();
				channels.put(address, channel);
			}
			else
				throw new AnalysisException("can't get lock to create channel");
		}
		catch(InterruptedException e)
		{
			//do nothing
		}
		finally
		{
			if (isLock)
				channelLock.unlock();
		}
		
		return channel;
	}

	@Override
	public void releaseResource() {
		try {
		    long timestamp = System.currentTimeMillis();
		    while(!responseQueue.isEmpty() && (System.currentTimeMillis() - timestamp < 60000)) {
		        if(logger.isInfoEnabled()) {
		            logger.info("responseQueue has " + responseQueue.size() + " to receive");
		        }
		        Thread.sleep(1000);
		    }
//			if (slaveEventTimeQueue != null)
//				slaveEventTimeQueue.release();

			for(Channel channel : channels.values())
			{
				try
				{
					channel.getCloseFuture().cancel();
					channel.getCloseFuture().await(3000);
				}
				catch(Exception ex)
				{
					logger.error(ex);
				} finally {
				    channel.close();
				}
			}
					
			factory.releaseExternalResources();
			responseQueue.clear();
		} catch (Exception ex) {
			logger.error(ex, ex);
		}

		logger.info("SocketSlaveConnector releaseResource now.");
	}

	@Override
	public JobTask[] getJobTasks(GetTaskRequestEvent requestEvent) {
		JobTask[] tasks = null;

		try {
			final GetTaskRequestEvent event = requestEvent;
			// 简单的用这种模式模拟阻塞请求
			if(logger.isInfoEnabled())
			    logger.info("trying to get tasks from master " + requestEvent.getRequestJobCount());
			responseQueue.put(requestEvent.getSequence(), requestEvent);
//			slaveEventTimeQueue.add(requestEvent);
			
			Channel channel = getChannel(leaderChannel);
			
			requestEvent.setChannel(channel);
			
			ChannelFuture channelFuture = channel.write(requestEvent);

			// why ??, 这里不必等待
			//channelFuture.await(10, TimeUnit.SECONDS);

			channelFuture.addListener(new ChannelFutureListener() {
				public void operationComplete(ChannelFuture future) {
					if (!future.isSuccess()) {
						responseQueue.remove(event.getSequence());
//						slaveEventTimeQueue.remove(event);
						
						logger.error("Slavesocket write error when trying to get tasks from master.",
								future.getCause());
						future.getChannel().close();
					}
				}
			});
			
			// 在SlaveConnectorHandler. messageReceived中会countDown
			requestEvent.getResultReadyFlag().await(
					config.getMaxClientEventWaitTime(), TimeUnit.SECONDS);

			GetTaskResponseEvent responseEvent = (GetTaskResponseEvent) requestEvent
					.getResponse();

			if (responseEvent != null && responseEvent.getJobTasks() != null
					&& responseEvent.getJobTasks().size() > 0) {
				tasks = new JobTask[responseEvent.getJobTasks().size()];
				responseEvent.getJobTasks().toArray(tasks);
			}

		} catch (Exception ex) {
			logger.error(ex, ex);
		}

		return tasks;
	}
	
	@Override
	public String sendJobTaskResults(final SendResultsRequestEvent jobResponseEvent, final String master) {
	    try 
        {
            final SendResultsRequestEvent event = jobResponseEvent;
            
            // 简单的用这种模式模拟阻塞请求
            responseQueue.put(jobResponseEvent.getSequence(), jobResponseEvent);
//            slaveEventTimeQueue.add(jobResponseEvent);
            
            Channel channel = getChannel(master);
            jobResponseEvent.setChannel(channel);
            final long start = System.currentTimeMillis();
            ChannelFuture channelFuture = channel.write(jobResponseEvent);
//            channelFuture.await(10, TimeUnit.SECONDS);

            channelFuture.addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future) {
                    if (!future.isSuccess()) {
                        responseQueue.remove(event.getSequence());
//                        slaveEventTimeQueue.remove(event);
                        
                        logger.error("Slavesocket write error when send task result to master. tasks:"
                                + event.getJobTaskResult().toString() + ",use:" + (System.currentTimeMillis() - start),
                            future.getCause());
                        future.getChannel().close();
                    } else {
                        if (logger.isWarnEnabled()) {
                            logger.warn("send task success" + jobResponseEvent.getJobTaskResult().toString()
                                    + " result to master " + master + ",use:" + (System.currentTimeMillis() - start));
                        }
                    }
                }
            });

            jobResponseEvent.getResultReadyFlag().await(
                    config.getMaxClientEventWaitTime(), TimeUnit.SECONDS);

            SendResultsResponseEvent responseEvent = (SendResultsResponseEvent) jobResponseEvent
                    .getResponse();

            if (responseEvent != null) {
                return responseEvent.getResponse();
            }
        } catch (Exception ex) {
            logger.error("sendJobTaskResults error,master address : " + master,ex);
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

	/*
	class SlaveEventTimeOutQueue extends TimeOutQueue<MasterNodeEvent> {
		@Override
		public void timeOutAction(MasterNodeEvent event) {
			if (responseQueue.containsKey(event.getSequence()))
			{
				responseQueue.remove(event.getSequence());
				
				logger.info("SlaveEventTimeOutQueue remove event : " + event.getSequence());
				
				//在发送超时后，关闭当前通道
				if(event.getChannel() != null)
				    ((Channel)event.getChannel()).close();
			}
		}
	}
	*/

	@Override
	public void changeMaster(String master) {
		this.leaderChannel = master;
	}
}
