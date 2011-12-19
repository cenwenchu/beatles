package com.taobao.top.analysis.statistics.map;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;

/**
 * 通过final限制mapper的流程
 * @author zhudi
 *
 */
public abstract class AbstractMapper implements IMapper {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7568020685361775161L;
	/**
	 * 通过entry的Condition对象过滤，实现generatekey方法
	 */
	@Override
	public final String mapperKey(ReportEntry entry, String[] contents, JobTask jobtask) {
		if(entry.getCondition().isInCondition(contents)){
			return this.generateKey(entry, contents, jobtask);
		}
		
		return null;
	}

	protected abstract String generateKey(ReportEntry entry,String[] contents, JobTask jobtask);
	
	protected abstract Object generateValue(ReportEntry entry,Object[] contents, JobTask jobtask);
	/**
	 * 通过实现类的generateValue方法生成value，通过entry的filter对象filter具体的value值
	 */
	@Override
	public final Object mapperValue(ReportEntry entry, Object[] contents,
			JobTask jobtask) {
		Object value = generateValue(entry, contents, jobtask);
		return entry.getValueFilter().filter(value);
	}

}
