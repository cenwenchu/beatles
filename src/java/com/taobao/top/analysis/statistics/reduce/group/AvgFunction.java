package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

import com.taobao.top.analysis.util.AnalysisConstants;

public class AvgFunction implements GroupFunction {

	@Override
	public void group(String key, Object value, Map<String, Object> result) {
		if (value == null)
			return;
		value = Double.parseDouble(value.toString());
		String sumkey = new StringBuilder().append(AnalysisConstants.PREF_SUM).append(key)
				.toString();
		String countkey = new StringBuilder().append(AnalysisConstants.PREF_COUNT).append(key)
				.toString();
		Double sum = (Double) result.get(sumkey);
		Double count = (Double) result.get(countkey);
		if (sum == null || count == null) {
			result.put(sumkey, (Double) value);
			result.put(countkey, (Double) 1.0);
			result.put(key, (Double) value);
		} else {
			// 再次验证一下
			Object tempvalue = ((Double) value + sum)
					/ (Double) (count + 1);
			result.put(sumkey, (Double) value + sum);
			result.put(countkey, (Double) (count + 1));
			result.put(key, tempvalue);
		}
	}

}
