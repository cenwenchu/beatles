package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

public class CountFunction implements GroupFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -627834910678760568L;

	@Override
	public void group(String key, Object value, Map<String, Object> result) {
		Double total = (Double) result.get(key);
		if(value == null){
			value = 1.0;
		}
		if (total == null)
			result.put(key, value);
		else
			result.put(key, total + (Double)value);
	}

}
