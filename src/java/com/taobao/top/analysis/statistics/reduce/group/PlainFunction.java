package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;

public class PlainFunction implements GroupFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6070415403674804310L;

	@Override
	public void group(ReportEntry entry,String key, Object value, Map<String, Object> result,ReduceType rs) {
		Object o = result.get(key);

		if (o == null)
			result.put(key, value);
	}

}
