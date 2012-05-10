package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;

import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;

public class MinFunction implements GroupFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5755380133525364196L;

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

		Double min = (Double) result.get(key);

		if (min == null)
			result.put(key, (Double) value);
		else if ((Double) value < min)
			result.put(key, (Double) value);
	}

}
