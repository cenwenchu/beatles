package com.taobao.top.analysis.statistics.reduce.group;

import java.util.Map;

import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.reduce.IReducer.ReduceType;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * 直到export的时候才真正执行average操作
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午11:17:47
 *
 */
public class AvgFunction implements GroupFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9022021538712756593L;

	@Override
	public void group(ReportEntry entry,String key, Object value, Map<String, Object> result,ReduceType rs) {
		if (value == null)
			return;
		value = Double.parseDouble(value.toString());
		
		
		if(key.startsWith(AnalysisConstants.PREF_SUM)) 
		{
			Double sum = (Double) result.get(key);
			
			if (sum == null) {
    			result.put(key, (Double) value);			
    		} else {	
    			result.put(key, (Double) value + sum);
    		}
        } 
		else 
			if(key.startsWith(AnalysisConstants.PREF_COUNT)) 
			{
				Double count = (Double) result.get(key);
				
				if (count == null) {
	    			result.put(key, (Double) value);			
	    		} else {	
	    			result.put(key, (Double) value + count);
	    		}
	        }		
	        else
	        {
	        	String sumkey = new StringBuilder().append(AnalysisConstants.PREF_SUM).append(key)
	    				.toString();
	    		String countkey = new StringBuilder().append(AnalysisConstants.PREF_COUNT).append(key)
	    				.toString();
	    		Double sum = (Double) result.get(sumkey);
	    		Double count = (Double) result.get(countkey);
	    		
	    		if (sum == null || count == null) {
	    			result.put(sumkey, (Double) value);
	    			result.put(countkey, (Double) 1.0);			
	    		} else {	
	    			result.put(sumkey, (Double) value + sum);
	    			result.put(countkey, (Double) (count + 1));
	    		}
	        }
	
	}

}
