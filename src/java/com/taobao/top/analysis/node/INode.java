/**
 * 
 */
package com.taobao.top.analysis.node;

import com.taobao.top.analysis.config.IConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.event.INodeEvent;

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
	public void process() throws AnalysisException;
	
		
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
	public void processEvent(E event) throws AnalysisException;
	
	/**
	 * 启动节点
	 */
	public void startNode();
	
	/**
	 * 停止节点
	 */
	public void stopNode();
	
}
