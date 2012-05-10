package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;

import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;


public class SumFunction implements GroupFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4958058432655840945L;

	@Override
	public void group(ReportEntry entry,String key, Object value, Map<String, Object> result,ReduceType rs) {

		if (value == null)
			return;

		if (value instanceof String) {
		    if(NumberUtils.isNumber((String)value))
		        value = Double.parseDouble((String) value);
		    else 
		        value = 0d;
		} else if (value instanceof Double) {
            value = (Double) value;
        } else {
            value = Double.parseDouble(String.valueOf(value));
        }

		Double _sum = (Double) result.get(key);

		if (_sum == null)
			result.put(key, (Double) value);
		else
			result.put(key, (Double) value + _sum);
	}

}
