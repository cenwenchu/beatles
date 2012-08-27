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
import com.taobao.top.analysis.node.event.GetTaskRequestEvent;
import com.taobao.top.analysis.node.event.MasterEventCode;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SendMonitorInfoEvent;
import com.taobao.top.analysis.node.event.SendResultsRequestEvent;

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
					nodeEvent.getEventCode().equals(MasterEventCode.SEND_RESULT) || nodeEvent.getEventCode().equals(MasterEventCode.SEND_MONITOR_INFO))
			{
				nodeEvent.setChannel(channel);
				masterNode.addEvent(nodeEvent);
				
			} else {
			    if (logger.isInfoEnabled())
                    logger.info("receive message from slave : " + channel.getRemoteAddress() + ", squence : " + nodeEvent.getSequence());
			}
			
            if (nodeEvent.getEventCode().equals(MasterEventCode.GET_TASK)) {
                //INFO信息记录M/S通信内容
                if (logger.isInfoEnabled()) {
                    GetTaskRequestEvent requestEvent = (GetTaskRequestEvent) nodeEvent;
                    String jobName = requestEvent.getJobName();
                    int jobCount = requestEvent.getRequestJobCount();
                    StringBuffer stringBuffer = new StringBuffer("receive get_task event, jobName:");
                    if (jobName != null)
                        stringBuffer.append("jobName:").append(jobName).append(",");
                    stringBuffer.append("jobCount:").append(jobCount).append(",from:")
                        .append(channel.getRemoteAddress()).append(",squence:").append(nodeEvent.getSequence());
                    logger.info(stringBuffer.toString());
                }
            }
			if(nodeEvent.getEventCode().equals(MasterEventCode.SEND_RESULT)) {
			    //INFO信息记录M/S通信内容
			    if(logger.isInfoEnabled()) {
			        SendResultsRequestEvent requestEvent = (SendResultsRequestEvent)nodeEvent;
	                StringBuffer stringBuffer = new StringBuffer("receive send_result event, result:{");
	                stringBuffer.append(requestEvent.getJobTaskResult().toString()).append("}");
	                stringBuffer.append("from slave:").append(channel.getRemoteAddress()).append(",squence:").append(nodeEvent.getSequence());
			        logger.info(stringBuffer.toString());
			    }
			}
			if(nodeEvent.getEventCode().equals(MasterEventCode.SEND_MONITOR_INFO)) {
				 //INFO信息记录M/S通信内容
			    if(logger.isInfoEnabled()) {
			        SendMonitorInfoEvent requestEvent = (SendMonitorInfoEvent)nodeEvent;
	                StringBuffer stringBuffer = new StringBuffer("receive send_monitor_info event, result:{");
	                stringBuffer.append(requestEvent.getSlaveMonitorInfo().toString()).append("}");
	                stringBuffer.append("from slave:").append(channel.getRemoteAddress()).append(",squence:").append(nodeEvent.getSequence());
			        logger.info(stringBuffer.toString());
			    }
			}
		}
		
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		
		logger.error("Unexpected exception from downstream.channel:" + e.getChannel().getRemoteAddress().toString(),
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
