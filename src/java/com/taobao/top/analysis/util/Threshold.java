/**
 * 
 */
package com.taobao.top.analysis.util;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简单的一个实现，防止日志爆掉
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class Threshold implements Serializable{
	
	/**
     * 
     */
    private static final long serialVersionUID = -8426057843371774390L;
    private AtomicLong lastTimeStamp;
	private long frequency;
	
	public Threshold(long frequency)
	{
		this.frequency = frequency;
		this.lastTimeStamp = new AtomicLong(System.currentTimeMillis());
	}
	
	public boolean sholdBlock()
	{
		long now = System.currentTimeMillis();
		long interval =  now - lastTimeStamp.get();
		
		if (interval > frequency)
		{
			//这里不用并发原语
			lastTimeStamp.set(now);
			return false;
		}
		else
			return true;
	}

}
