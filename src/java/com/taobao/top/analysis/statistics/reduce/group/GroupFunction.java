package com.taobao.top.analysis.statistics.reduce.group;

import java.io.Serializable;
import java.util.Map;
/**
 * 组操作方法类
 * @author zhudi
 *
 */
public interface GroupFunction extends Serializable {
	
	public void group(String key,Object value,Map<String, Object> result);

}
