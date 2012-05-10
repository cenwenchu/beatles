package com.taobao.top.analysis.statistics.reduce.group;

import java.io.Serializable;
import java.util.Map;

import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;
/**
 * 组操作方法类
 * @author zhudi
 *
 */
public interface GroupFunction extends Serializable {
	
	public void group(ReportEntry entry,String key,Object value,Map<String, Object> result,ReduceType rs);

}
