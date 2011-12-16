package com.taobao.top.analysis.statistics.reduce;

import java.util.Map;

import com.taobao.top.analysis.statistics.data.ExpressionReportEntry;
/**
 * 
 * @author zhudi
 *
 */
public class DefaultExpressionReducer implements IReducer<ExpressionReportEntry> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5044773038902156077L;

	@Override
	public final void reducer(ExpressionReportEntry entry,String key,Object value,Map<String, Object> result) {
		entry.getGroupFunction().group(key, value, result);
	}

}
