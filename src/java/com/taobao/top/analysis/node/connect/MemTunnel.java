/**
 * 
 */
package com.taobao.top.analysis.node.connect;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.taobao.top.analysis.node.event.MasterNodeEvent;
import com.taobao.top.analysis.node.event.SlaveNodeEvent;

/**
 * 服务端和客户端交互通道
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class MemTunnel {
	
	public BlockingQueue<SlaveNodeEvent> slaveSide;
	
	public BlockingQueue<MasterNodeEvent> masterSide;
	
	public MemTunnel(){
		slaveSide = new LinkedBlockingQueue<SlaveNodeEvent>();
		masterSide = new LinkedBlockingQueue<MasterNodeEvent>();
	}


	
	public BlockingQueue<SlaveNodeEvent> getSlaveSide() {
		return slaveSide;
	}



	public void setSlaveSide(BlockingQueue<SlaveNodeEvent> slaveSide) {
		this.slaveSide = slaveSide;
	}



	public BlockingQueue<MasterNodeEvent> getMasterSide() {
		return masterSide;
	}



	public void setMasterSide(BlockingQueue<MasterNodeEvent> masterSide) {
		this.masterSide = masterSide;
	}



	public void sendToSlave(SlaveNodeEvent event)
	{
		slaveSide.offer(event);
	}
	
	public void sendToMaster(MasterNodeEvent event)
	{
		masterSide.offer(event);
	}

}
