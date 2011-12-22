package com.taobao.top.analysis.statistics.reduce;

import java.util.Map;

import com.taobao.top.analysis.statistics.data.ReportEntry;
/**
 * 
 * @author zhudi
 * 默认的实现类
 *
 */
public class DefaultReducer implements IReducer {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5044773038902156077L;

	@Override
	public final void reducer(ReportEntry entry,String key,Object value,Map<String, Object> result) {
		entry.getGroupFunction().group(key, value, result);
	}

}
