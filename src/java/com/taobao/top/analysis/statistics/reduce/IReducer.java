package com.taobao.top.analysis.statistics.reduce;

import java.io.Serializable;
import java.util.Map;

import com.taobao.top.analysis.statistics.data.ReportEntry;
/**
 * 
 * @author zhudi
 *
 * @param <T>
 */
public interface IReducer<T extends ReportEntry> extends Cloneable,Serializable {
	
	public void reducer(T entry,String key,Object value,Map<String, Object> result);
	

}
