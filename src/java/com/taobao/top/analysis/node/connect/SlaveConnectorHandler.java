/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.taobao.top.analysis.node.component.SlaveNode;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SlaveEventCode;
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
//	SlaveEventTimeOutQueue slaveEventTimeQueue;
	private SlaveNode slaveNode;
	volatile Channel channel;
	
	public SlaveConnectorHandler(Map<String,MasterNodeEvent> responseQueue, SlaveNode slaveNode)
	{
		super();
		this.responseQueue = responseQueue;
//		this.slaveEventTimeQueue = slaveEventTimeQueue;
		this.slaveNode = slaveNode;
	}
	
	@Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        channel = e.getChannel();
    }
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event)
			throws Exception {
		if (event instanceof ChannelStateEvent) 
		{
		    StringBuffer sb = new StringBuffer(event.toString());
		    sb.append(",pipelines");
		    for(int i=0; i<ctx.getPipeline().getNames().size(); i++) {
		        sb.append(ctx.getPipeline().getNames().get(i));
		    }
			logger.info(sb.toString());
		}
		
		super.handleUpstream(ctx, event);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		SlaveNodeEvent slaveEvent = (SlaveNodeEvent)e.getMessage();
		
		if (slaveEvent != null)
		{
		    if(SlaveEventCode.GET_TASK_RESP.equals(slaveEvent.getEventCode())) {
		        slaveNode.addEvent(slaveEvent);
		    }
			if (responseQueue.containsKey(slaveEvent.getSequence()))
			{
				responseQueue.get(slaveEvent.getSequence()).setResponse(slaveEvent);
				responseQueue.get(slaveEvent.getSequence()).getResultReadyFlag().countDown();
				
//				if(!slaveEventTimeQueue.remove(responseQueue.get(slaveEvent.getSequence())))
//					logger.error("event not in timeout queue, please check code,maybe it be wrong!");
				
				responseQueue.remove(slaveEvent.getSequence());
			}
			else
				logger.error("receive invalidate response,sequence :" + slaveEvent.getSequence());
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		logger.error("Unexpected exception from downstream.",
				                 e.getCause());
		logger.error(ctx.getAttachment());
		if(e.getChannel().isOpen()) {
		    logger.error("close channel");
		    e.getChannel().close();
		}
	}

}
