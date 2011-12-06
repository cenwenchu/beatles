/**
 * 
 */
package com.taobao.top.analysis.util;

/**
 * 超时队列中放置的数据
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-5 下午1:51:35
 *
 */
public interface TimeOutEvent extends java.lang.Comparable<TimeOutEvent> {
	
	/**
	 * 获得创建时间，long类型，毫秒
	 * @return
	 */
	public long getEventCreateTime();
	
	public void setEventCreateTime(long eventCreateTime);
	
	/**
	 * 当前事件最大有效期，单位秒
	 * @return
	 */
	public long getMaxEventHoldTime();
	
	public void setMaxEventHoldTime(long maxEventHoldTime);
}
