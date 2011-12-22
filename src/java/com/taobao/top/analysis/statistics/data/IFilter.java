package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;

/**
 * 
 * @author zhudi
 *
 */
public interface IFilter extends Cloneable,Serializable{
	/**
	 * 生成了value之后用来过滤生成的value的
	 * @param value
	 * @return
	 */
	public Object filter(Object value);

}
