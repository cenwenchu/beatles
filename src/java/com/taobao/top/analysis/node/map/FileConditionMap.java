/**
 * 
 */
package com.taobao.top.analysis.node.map;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;

/**
 * 用于对多个报表字段有&的匹配需求，例如需要appkey在某一个范围并且api也在某一个范围
 * 
 * @author fangweng
 * 
 */
public class FileConditionMap implements IReportMap {

	static Map<String, List<String>> condition;

	public String generateKey(ReportEntry entry, String[] contents,
			Map<String, Alias> aliasPool, String tempMapParams,List<InnerKey> innerKeyPool) {
		String key = ReportUtil.generateKey(entry, contents,innerKeyPool);

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
