/**
 * 
 */
package com.taobao.top.analysis.node.connect;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.taobao.top.analysis.node.component.MasterNode;
import com.taobao.top.analysis.node.event.MasterEventCode;
import com.taobao.top.analysis.node.event.MasterNodeEvent;

/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-5 上午11:44:50
 *
 */
public class MasterConnectorHandler extends SimpleChannelUpstreamHandler {
	
	private static final Log logger = LogFactory.getLog(MasterConnectorHandler.class);
	
	MasterNode masterNode;
	volatile Channel channel;
	
	public MasterConnectorHandler(MasterNode masterNode)
	{
		super();
		this.masterNode = masterNode;
	}
	
	@Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        channel = e.getChannel();
    }
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		
		
		MasterNodeEvent nodeEvent = (MasterNodeEvent)e.getMessage();
		
		if (nodeEvent != null)
		{
			if (nodeEvent.getEventCode().equals(MasterEventCode.GET_TASK) || 
					nodeEvent.getEventCode().equals(MasterEventCode.SEND_RESULT))
			{
				nodeEvent.setChannel(channel);
				masterNode.addEvent(nodeEvent);
				
				if (logger.isInfoEnabled())
					logger.info("receive message from slave, squence : " + nodeEvent.getSequence());
			}
		}
		
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		
		logger.warn("Unexpected exception from downstream.",
                e.getCause());
		e.getChannel().close();
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

}
