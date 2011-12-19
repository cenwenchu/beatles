package com.taobao.top.analysis.statistics.reduce;

import java.io.Serializable;
import java.util.Map;

import com.taobao.top.analysis.statistics.data.ReportEntry;
/**
 * 
 * @author zhudi
 * mapper操作生成的key和value会通过调用这个方法聚合
 *
 * @param <T>
 */
public interface IReducer extends Cloneable,Serializable {
	/**
	 * 聚合
	 * @param entry
	 * @param key
	 * @param value
	 * @param result
	 */
	public void reducer(ReportEntry entry,String key,Object value,Map<String, Object> result);
	

}
