package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

public class PlainFunction implements GroupFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6070415403674804310L;

	@Override
	public void group(String key, Object value, Map<String, Object> result) {
		Object o = result.get(key);

		if (o == null)
			result.put(key, value);
	}

}
