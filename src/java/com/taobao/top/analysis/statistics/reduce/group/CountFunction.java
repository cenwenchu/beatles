package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;

public class CountFunction implements GroupFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -627834910678760568L;

	@Override
	public void group(ReportEntry entry,String key, Object value, Map<String, Object> result,ReduceType rs) {
	    
		Double total = 0d;
		try {
		    total = (Double) result.get(key);
		} catch (Throwable e) {
		}
		if(value == null){
			value = 1.0;
		} else if(!(value instanceof Double))
		    value = Double.valueOf(String.valueOf(value)); 
		    
		if(((Double)value).isNaN()) {
            return;
        }
		if (total == null)
			result.put(key, value);
		else
			result.put(key, total + (Double)value);
	}

}
