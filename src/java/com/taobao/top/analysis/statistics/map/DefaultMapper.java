package com.taobao.top.analysis.statistics.map;

import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;
/**
 * 
 * @author zhudi
 * 默认的mapper实现
 *
 */
public class DefaultMapper extends AbstractMapper {

	/**
	 * 
	 */
	private static final long serialVersionUID = 697288673757584978L;

	
	@Override
	protected String generateKey(ReportEntry entry,String[] contents, JobTask jobtask){
		StringBuilder key = new StringBuilder();
		for (int c : entry.getKeys()) {
			// 全局统计，没有key
			if (c == AnalysisConstants.GLOBAL_KEY)
				return AnalysisConstants.GLOBAL_KEY_STR;

			key.append(innerKeyReplace(c,contents[c - 1],jobtask.getStatisticsRule().getInnerKeyPool())).append(AnalysisConstants.SPLIT_KEY);
			
		}
		return key.toString().intern();
	}
	
	private static String innerKeyReplace(int key,String value,List<InnerKey> innerKeyPool)
	{
		String result = value;
		
		if (innerKeyPool == null || (innerKeyPool != null && innerKeyPool.size() == 0))
			return result;
		
		for(InnerKey ik : innerKeyPool)
		{
			if (ik.getKey() == key)
			{
				if (ik.getInnerKeys().get(value) != null)
					result = ik.getInnerKeys().get(value);
				
				break;
			}
		}
		
		return result;
	}

	@Override
	protected Object generateValue(ReportEntry entry,
			Object[] contents, JobTask jobtask) {
		return entry.getCalculator().calculator(contents);
	}
	
}
