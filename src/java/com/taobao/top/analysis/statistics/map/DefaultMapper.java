package com.taobao.top.analysis.statistics.map;

import java.util.Iterator;
import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.ObjectColumn;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;
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
    protected String generateKey(ReportEntry entry, String[] contents, JobTask jobtask) {
        StringBuilder key = new StringBuilder();
        
        Iterator<ObjectColumn> subkeys = null;
        
        if (entry.getSubKeys() != null)
        	subkeys = entry.getSubKeys().iterator();
        
        for (int c : entry.getKeys()) {
            // 全局统计，没有key
            if (c == AnalysisConstants.GLOBAL_KEY)
                return AnalysisConstants.GLOBAL_KEY_STR;
            
            if (c == AnalysisConstants.Object_KEY)
            {
            	ObjectColumn oc = subkeys.next();
            	
            	if (oc == null)
            		throw new java.lang.RuntimeException(new StringBuilder(entry.getName()).append(" objectColumn not exist!").toString());
            	
            	String column = contents[oc.getcIndex() - 1];
            	
            	return ReportUtil.getValueFromJosnObj(column,oc.getSubKeyName());
            	
            }

            if (c > contents.length) {
                if (!threshold.sholdBlock())
                    logger.error(new StringBuilder().append("Entry :").append(entry.getId()).append(", job : ")
                        .append(jobtask.getJobName()).append(", entry:").append(entry.getName()).append(", index:")
                        .append(c).append("\r record: ").append(contents.length).toString());
                return null;
            }
            key.append(innerKeyReplace(c, contents[c - 1], jobtask.getStatisticsRule().getInnerKeyPool())).append(
                AnalysisConstants.SPLIT_KEY);

        }
        return key.toString();
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
