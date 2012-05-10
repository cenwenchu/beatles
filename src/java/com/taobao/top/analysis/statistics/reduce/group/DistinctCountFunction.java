/**
 * 
 */
package com.taobao.top.analysis.statistics.reduce.group;


import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.statistics.data.DistinctCountEntryValue;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.bloom.ByteBloomFilter;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午4:51:14
 *
 */
public class DistinctCountFunction implements GroupFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7255142962029484084L;
	
	private static final Log logger = LogFactory.getLog(DistinctCountFunction.class);

	@Override
	public void group(ReportEntry entry,String key, Object value, Map<String, Object> result,ReduceType rs) {
		
		if (value == null)
			return;
		
		//浅层合并的时候，不做bloom过滤器
		if (rs == ReduceType.SHALLOW_MERGE)
		{
			String nkey;
			
			if (key.startsWith(AnalysisConstants.MAGIC_NUM))
				nkey = key;
			else
				nkey = new StringBuilder(AnalysisConstants.MAGIC_NUM).append(key).append(value).toString();
			
			if (!result.containsKey(nkey))
				result.put(nkey, value);
			
			return;
		}
		else
		{
			//一种情况是result里面还是原生态的数据，不是bloom过滤器，则需要构建bloom过滤器		
			String nkey = key.substring(AnalysisConstants.MAGIC_NUM.length(), key.length() - value.toString().length());
			DistinctCountEntryValue distinctEntry = (DistinctCountEntryValue)result.get(nkey);
			
			if (distinctEntry == null)
			{
				distinctEntry = createDCEntryValue(entry);
				result.put(nkey, distinctEntry);
			}
			
			try
			{
				//存在一定危险性，key与nkey冲突
				if (result.get(key) != null && !(result.get(key) instanceof DistinctCountEntryValue))
				{
					distinctEntry.add(result.get(key).toString());
					result.remove(key);
				}
				
				distinctEntry.add(value.toString());
			}
			catch(Exception ex)
			{
				logger.error(ex);
			}
			
		}
		
	}
	
	DistinctCountEntryValue createDCEntryValue(ReportEntry entry)
	{
		DistinctCountEntryValue distinctEntryValue = new DistinctCountEntryValue();
		ByteBloomFilter bloomFilter;
		
		//240k
		int maxKeys = 100000;
		float errorRate = 0.0001F;
		
		if (entry.getAdditions().get(AnalysisConstants.ANALYSIS_BLOOM_MAXKEYS) != null)
			maxKeys = (Integer)entry.getAdditions().get(AnalysisConstants.ANALYSIS_BLOOM_MAXKEYS);
		
		if (entry.getAdditions().get(AnalysisConstants.ANALYSIS_BLOOM_ERRORRATE) != null)
			errorRate = (Float)entry.getAdditions().get(AnalysisConstants.ANALYSIS_BLOOM_ERRORRATE);
			
		bloomFilter = new ByteBloomFilter(maxKeys,errorRate,1);
		
		distinctEntryValue.setBloomFilter(bloomFilter);
		
		return distinctEntryValue;
	}

}
