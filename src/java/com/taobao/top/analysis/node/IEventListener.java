/**
 * 
 */
package com.taobao.top.analysis.node;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IEventListener< E extends INodeEvent> {
	
	public void action(E event);

}
