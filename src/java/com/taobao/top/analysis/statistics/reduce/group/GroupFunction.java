package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

public interface GroupFunction {
	
	public void group(String key,Object value,Map<String, Object> result);

}
