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
public interface IReducer extends Cloneable,Serializable {
	
	public void reducer(ReportEntry entry,String key,Object value,Map<String, Object> result);
	

}
