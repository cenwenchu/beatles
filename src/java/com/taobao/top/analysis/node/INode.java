/**
 * 
 */
package com.taobao.top.analysis.node;

/**
 * 基础节点的接口定义
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface INode<E extends INodeEvent> {
	
	public void init();
	
	public void destory();
		
	public void addEventListener(IEventListener<E> eventListener);
	
	public void removeEventListener(IEventListener<E> eventListener);

}
