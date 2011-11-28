/**
 * 
 */
package com.taobao.top.analysis.node;

import com.taobao.top.analysis.config.IConfig;

/**
 * 基础节点的接口定义
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface INode<E extends INodeEvent,C extends IConfig> extends Runnable,IComponent<C>{
		
	/**
	 * 节点的主流程处理逻辑
	 */
	public void process();
	
		
	/**
	 * 向Node发送消息
	 * @param event
	 * @return
	 */
	public boolean addEvent(E event);
	
	/**
	 * 节点如何处理事件的实现
	 * @param event
	 */
	public void processEvent(E event);
	
}
