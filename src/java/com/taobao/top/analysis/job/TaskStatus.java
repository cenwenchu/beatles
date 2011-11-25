/**
 * 
 */
package com.taobao.top.analysis.job;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public enum TaskStatus {
	
	UNDO("undo"),
	DOING("doing"),
	DONE("done"),
	MERGED("merged");
	
	String v;
	
	TaskStatus(String v)
	{
		this.v = v;
	}
	
	@Override
	public String toString() {
		return v;
	}

}
