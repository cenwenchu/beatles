package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

public class MinFunction implements GroupFunction {

	@Override
	public void group(String key, Object value, Map<String, Object> result) {
		if (value == null)
			return;

		if (value instanceof String) {
			value = Double.parseDouble((String) value);
		}

		Double min = (Double) result.get(key);

		if (min == null)
			result.put(key, (Double) value);
		else if ((Double) value < min)
			result.put(key, (Double) value);
	}

}
