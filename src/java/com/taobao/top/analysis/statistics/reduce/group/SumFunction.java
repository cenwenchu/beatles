package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

public class SumFunction implements GroupFunction {

	@Override
	public void group(String key, Object value, Map<String, Object> result) {

		if (value == null)
			return;

		if (value instanceof String) {
			value = Double.parseDouble((String) value);
		}

		Double _sum = (Double) result.get(key);

		if (_sum == null)
			result.put(key, (Double) value);
		else
			result.put(key, (Double) value + _sum);
	}

}
