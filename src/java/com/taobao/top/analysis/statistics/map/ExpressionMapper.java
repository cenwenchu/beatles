package com.taobao.top.analysis.statistics.map;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ExpressionReportEntry;

public abstract class ExpressionMapper<T extends ExpressionReportEntry> implements IMapper<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3146502656551300145L;

	@Override
	public final String mapperKey(T entry, String[] contents,
			JobTask jobtask) {
		if(isInCondition(entry, contents, jobtask)){
			return generateKey(entry, contents, jobtask);
		}
		return null;
	}
	
	protected abstract String generateKey(T entry,String[] contents, JobTask jobtask);
	protected abstract boolean isInCondition(T entry,String[] contents, JobTask jobtask);
	
	protected abstract boolean isNeedFilter(T entry,Object value, JobTask jobtask);
	protected abstract Object generateValue(T entry,Object[] contents, JobTask jobtask);

	@Override
	public final Object mapperValue(T entry, Object[] contents,
			JobTask jobtask) {
		Object value = generateValue(entry, contents, jobtask);
		if(!isNeedFilter(entry, value, jobtask)){
			return value;
		}
		return null;
	}

}
