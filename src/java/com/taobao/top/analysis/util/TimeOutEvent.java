/**
 * 
 */
package com.taobao.top.analysis.util;

/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-5 下午1:51:35
 *
 */
public interface TimeOutEvent extends java.lang.Comparable<TimeOutEvent> {
	
	public long getEventCreateTime();
	
	public void setEventCreateTime(long eventCreateTime);
	
	public long getMaxEventHoldTime();
	
	public void setMaxEventHoldTime(long maxEventHoldTime);
}
