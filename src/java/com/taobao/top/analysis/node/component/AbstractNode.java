/**
 * 
 */
package com.taobao.top.analysis.node.component;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.IConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.INode;
import com.taobao.top.analysis.node.event.INodeEvent;


/**
 * 抽象节点处理主骨骼结构，当前节点循环执行某一些逻辑，后台也支持监听事件发生
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public abstract class AbstractNode<E extends INodeEvent,C extends IConfig> implements INode<E,C>{
	
	private static final Log logger = LogFactory.getLog(AbstractNode.class);
	
	boolean running = true;
	
	protected C config;
	
	/**
	 * 消息队列
	 */
	private BlockingQueue<E> queue;
	
	/**
	 * 后台监控者，来接受事件，处理外部消息
	 */
	private Inspector inspector;
	
	private Thread innerThread;
	
	
	public AbstractNode()
	{
		queue = new LinkedBlockingQueue<E>();
		inspector = new Inspector();
		inspector.start();
	}
	
	/**
	 * 启动节点
	 */
	@Override
	public void startNode()
	{
		innerThread = new Thread(this);
		innerThread.start();
	}
	
	/**
	 * 停止节点
	 */
	@Override
	public void stopNode()
	{
		running = false;
		innerThread.interrupt();
	}

	public C getConfig() {
		return config;
	}

	public void setConfig(C config) {
		this.config = config;
	}

	@Override
	public void run() {
		try
		{
			this.init();
			
			while(running)
			{
				this.process();
			}
		}
		catch(AnalysisException ex)
		{
			logger.error(ex);
		}
		finally
		{
			this.inspector.stopInspector();
			this.releaseResource();
			
			logger.info("Node stopped ...");
		}
	}
	
	@Override
	public boolean addEvent(E event)
	{
		return queue.offer(event);
	}
	
	private class Inspector extends Thread
	{

		@Override
		public void run() {
			while(running)
			{
				try
				{
					E event = queue.poll(1000, TimeUnit.MILLISECONDS);
					
					if (event != null)
					{
						processEvent(event);
					}
					
				}
				catch(InterruptedException e)
				{
					//do nothing
				}
				catch(Exception ex)
				{
					logger.error(ex);
				}
			}
			
			logger.warn("Node Inspector stop now.");
		}
		
		public void stopInspector()
		{
			this.interrupt();
		}
		
	}

}
