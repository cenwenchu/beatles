/**
 * 
 */
package com.taobao.top.analysis.statistics.map;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * 用于对多个报表字段有&的匹配需求，例如需要appkey在某一个范围并且api也在某一个范围
 * 功能暂时不支持
 * 
 * @author fangweng
 * 
 */
public class FileConditionMapper extends DefaultMapper{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1362373610372800182L;
	static Map<String, List<String>> condition;

	@Override
	public String generateKey(ReportEntry entry, String[] contents, JobTask jobtask) {
		String key = super.generateKey(entry, contents, jobtask);
		Map<String, Alias> aliasPool = jobtask.getStatisticsRule().getAliasPool();
		if (AnalysisConstants.IGNORE_PROCESS.equals(key))
			return AnalysisConstants.IGNORE_PROCESS;

		if (condition != null && condition.size() > 0) {
			Iterator<String> conKeys = condition.keySet().iterator();

			while (conKeys.hasNext()) {
				String k = conKeys.next();

				int position = aliasPool.get(k).getKey();
				String conValue = contents[position - 1];

				if (!condition.get(k).contains(conValue))
					return AnalysisConstants.IGNORE_PROCESS;
			}
		}

		return key;
	}

	
	
}
