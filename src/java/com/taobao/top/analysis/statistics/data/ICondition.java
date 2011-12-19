package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;

/**
 * 
 * @author zhudi
 *
 */
public interface ICondition extends Cloneable,Serializable {
	
	
	public boolean isInCondition(String[] contents);

}
