package com.taobao.top.analysis.statistics.reduce.group;

import java.io.Serializable;
import java.util.Map;

public interface GroupFunction extends Serializable {
	
	public void group(String key,Object value,Map<String, Object> result);

}
