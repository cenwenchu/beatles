/**
 * 
 */
package com.taobao.top.analysis.node.job;

/**
 * 
 * 任务执行的状态，用于任务管理和分发（主要在服务端）
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public enum JobTaskStatus {
	
	UNDO("undo"),
	DOING("doing"),
	DONE("done"),
	MERGED("merged");
	
	String v;
	
	JobTaskStatus(String v)
	{
		this.v = v;
	}
	
	@Override
	public String toString() {
		return v;
	}

}
