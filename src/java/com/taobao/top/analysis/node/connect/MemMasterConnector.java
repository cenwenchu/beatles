/**
 * 
 */
package com.taobao.top.analysis.node.connect;


import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.MasterEventCode;
import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;

/**
 * 用于单机的分布式模拟，采用内存作为通信的服务端实现
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class MemMasterConnector extends AbstractMasterConnector implements Runnable {
	private final Log logger = LogFactory.getLog(MemMasterConnector.class);
	
	MemTunnel tunnel;
	boolean running;
	Thread innerThread;
	
	
	@Override
	public void init() throws AnalysisException {
		running = true;
		innerThread = new Thread(this);
		innerThread.start();
	}

	
	@Override
	public void releaseResource() {
		running = false;
		innerThread.interrupt();
	}

	public MemTunnel getTunnel() {
		return tunnel;
	}


	public void setTunnel(MemTunnel tunnel) {
		this.tunnel = tunnel;
	}


	@Override
	public void run() {
		
		while(running)
		{
			try
			{
				MasterNodeEvent nodeEvent = tunnel.getMasterSide().poll(1, TimeUnit.SECONDS);
				
				if (nodeEvent != null)
				{
					if (nodeEvent.getEventCode().equals(MasterEventCode.GET_TASK) || 
							nodeEvent.getEventCode().equals(MasterEventCode.SEND_RESULT))
					{
						masterNode.addEvent(nodeEvent);
					}
				}
				
			}
			catch (InterruptedException e) 
			{
				//do nothing
			}
			catch(Exception ex)
			{
				logger.error(ex);
			}
		}
	}


	@Override
	public void echoGetJobTasks(GetTaskResponseEvent event) {
		tunnel.getSlaveSide().offer(event);
	}


	@Override
	public void echoSendJobTaskResults(SendResultsResponseEvent event) {
		tunnel.getSlaveSide().offer(event);
	}

}
