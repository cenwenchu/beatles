package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;

/**
 * 
 * @author zhudi
 *
 */
public interface IFilter extends Cloneable,Serializable{
	
	public Object filter(Object value);

}
