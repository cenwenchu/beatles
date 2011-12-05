/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.taobao.top.analysis.node.connect.SocketSlaveConnector.SlaveEventTimeOutQueue;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SlaveNodeEvent;


/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-5 上午11:43:43
 *
 */
public class SlaveConnectorHandler extends SimpleChannelUpstreamHandler {
	
	private static final Log logger = LogFactory.getLog(SlaveConnectorHandler.class);
	
	Map<String,MasterNodeEvent> responseQueue;
	SlaveEventTimeOutQueue slaveEventTimeQueue;
	
	public SlaveConnectorHandler(Map<String,MasterNodeEvent> responseQueue,SlaveEventTimeOutQueue slaveEventTimeQueue)
	{
		this.responseQueue = responseQueue;
		this.slaveEventTimeQueue = slaveEventTimeQueue;
	}
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event)
			throws Exception {
		if (event instanceof ChannelStateEvent) 
		{
			logger.info(event.toString());
		}
		
		super.handleUpstream(ctx, event);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		SlaveNodeEvent slaveEvent = (SlaveNodeEvent)e.getMessage();
		
		if (slaveEvent != null)
		{
			if (responseQueue.containsKey(slaveEvent.getSequence()))
			{
				responseQueue.get(slaveEvent.getSequence()).setResponse(slaveEvent);
				responseQueue.get(slaveEvent.getSequence()).getResultReadyFlag().countDown();
				responseQueue.remove(slaveEvent.getSequence());
				
				if(!slaveEventTimeQueue.remove(slaveEvent))
					logger.error("event not in timeout queue, please check code,maybe it be wrong!");
			}
			else
				logger.error("receive invalidate response,sequence :" + slaveEvent.getSequence());
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		logger.warn("Unexpected exception from downstream.",
				                 e.getCause());
		e.getChannel().close();
	}

}
