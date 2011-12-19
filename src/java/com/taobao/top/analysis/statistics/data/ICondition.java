package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;

/**
 * 
 * @author zhudi
 *
 */
public interface ICondition extends Cloneable,Serializable {
	
	/**
	 * 生成key的时候用来过滤原始行数据的
	 * @param contents
	 * @return
	 */
	public boolean isInCondition(String[] contents);

}
