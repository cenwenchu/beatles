package com.taobao.top.analysis.statistics.map;

import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.util.AnalysisConstants;
/**
 * 
 * @author zhudi
 *
 */
public class DefaultExpressionMapper extends ExpressionMapper<DefaultExpReportEntry> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 697288673757584978L;

	
	@Override
	protected String generateKey(DefaultExpReportEntry entry,String[] contents, JobTask jobtask){
		StringBuilder key = new StringBuilder();
		for (int c : entry.getKeys()) {
			// 全局统计，没有key
			if (c == AnalysisConstants.GLOBAL_KEY)
				return AnalysisConstants.GLOBAL_KEY_STR;

			key.append(innerKeyReplace(c,contents[c - 1],jobtask.getStatisticsRule().getInnerKeyPool())).append(AnalysisConstants.SPLIT_KEY);
			
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
	protected boolean isInCondition(DefaultExpReportEntry entry,
			String[] contents, JobTask jobtask) {
		List<Object> conditionKStack = entry.getConditionKStack();
		List<Byte> conditionOpStack = entry.getConditionOpStack();
		List<Object> conditionVStack = entry.getConditionVStack();
		boolean andCondition = entry.isAndCondition();
		boolean checkResult = false;

		if (conditionKStack != null
				&& conditionKStack.size() > 0) {
			for (int i = 0; i < conditionKStack.size(); i++) {

				Object conditionKey = conditionKStack.get(i);
				byte operator = conditionOpStack.get(i);
				Object conditionValue = conditionVStack.get(i);
				int k = -1;

				// 长度condition特殊处理，没有指定的key列
				if (!conditionKey.equals(AnalysisConstants.RECORD_LENGTH)) {
					k = (Integer) conditionKey;
				}

				checkResult = checkKeyCondition(operator, k,
						conditionValue, contents);

				if (andCondition && !checkResult)
					return false;

				if (!andCondition && checkResult)
					return true;
			}
		}

		if (!andCondition && !checkResult)
			return false;
		return true;
	
	}
	
	/**
	 * 返回是否符合条件
	 * 
	 * @param operator
	 * @param conditionKey
	 * @param conditionValue
	 * @param contents
	 * @return
	 */
	private final static boolean checkKeyCondition(byte operator, int conditionKey,
			Object conditionValue, String[] contents) {
		boolean result = false;

		if (operator == AnalysisConstants.CONDITION_EQUAL) {
			if (conditionKey > 0)
				result = contents[conditionKey - 1].equals(conditionValue);
			else
				result = contents.length == (Integer)conditionValue;
		} else if (operator == AnalysisConstants.CONDITION_NOT_EQUAL) {
			if (conditionKey > 0)
				result = !contents[conditionKey - 1].equals(conditionValue);
			else
				result = contents.length != (Integer)conditionValue;
		} else {
			double cmpValue = 0;

			if (conditionKey > 0)
				cmpValue = Double.valueOf(contents[conditionKey - 1])
						- (Double)conditionValue;
			else
				cmpValue = contents.length - (Integer)conditionValue;

			if (operator == AnalysisConstants.CONDITION_EQUALORGREATER)
				return cmpValue >= 0;

			if (operator == AnalysisConstants.CONDITION_EQUALORLESSER)
				return cmpValue <= 0;

			if (operator == AnalysisConstants.CONDITION_GREATER)
				return cmpValue > 0;

			if (operator == AnalysisConstants.CONDITION_LESSER)
				return cmpValue < 0;

		}

		return result;
	}


	@Override
	protected boolean isNeedFilter(DefaultExpReportEntry entry, Object value,
			JobTask jobtask) {

		List<Object> valuefilterStack = entry.getValuefilterStack();
		List<Byte> valuefilterOpStack = entry.getValuefilterOpStack();
		boolean result = false;

		if (valuefilterStack == null
				|| (valuefilterStack != null && valuefilterStack.size() == 0))
			return result;

		try {
			for (int i = 0; i < valuefilterStack.size(); i++) {
				Object filterValue = valuefilterStack.get(i);
				Byte filterOpt = valuefilterOpStack.get(i);

				if (filterOpt == AnalysisConstants.CONDITION_ISNUMBER) {
					Double.parseDouble(value.toString());
				}

				if (filterOpt == AnalysisConstants.CONDITION_EQUAL) {
					if (value.equals(filterValue)) {
						continue;
					} else
						return true;
				}

				if (filterOpt == AnalysisConstants.CONDITION_EQUALORGREATER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v >= compareValue) {
						continue;
					} else
						return true;
				}

				if (filterOpt == AnalysisConstants.CONDITION_EQUALORLESSER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v <= compareValue) {
						continue;
					} else
						return true;
				}

				if (filterOpt == AnalysisConstants.CONDITION_GREATER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v > compareValue) {
						continue;
					} else
						return true;
				}

				if (filterOpt == AnalysisConstants.CONDITION_LESSER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v < compareValue) {
						continue;
					} else
						return true;
				}

				if (filterOpt == AnalysisConstants.CONDITION_NOT_EQUAL) {
					if (!value.equals(filterValue)) {
						continue;
					} else
						return true;
				}

			}
		} catch (Exception ex) {
			result = true;
		}

		return result;
	
	
	}

	@Override
	protected Object generateValue(DefaultExpReportEntry entry,
			Object[] contents, JobTask jobtask) {

		List<Object> bindingStack = entry.getBindingStack();
		List<Byte> operatorStack = entry.getOperatorStack();

		Object result = null;
		double left = 0;

		if (bindingStack != null
				&& bindingStack.size() > 0) {
			if (bindingStack.size() > 1) {
				if (bindingStack.get(0) instanceof String && ((String)bindingStack.get(0)).startsWith("#"))
					left = Double.valueOf(((String)bindingStack.get(0)).substring(1));
				else {
					if ((Integer)bindingStack.get(0) - 1 >= contents.length)
						return result;
					Object o = contents[(Integer)bindingStack.get(0) - 1];
					if(o instanceof Number){
						left = ((Number) o).doubleValue();
					}else{
						left = Double.valueOf(o.toString());
					}
					
				}

				double right = 0;

				int size = bindingStack.size();

				for (int i = 0; i < size - 1; i++) {
					if (bindingStack.get(i + 1) instanceof String && ((String)bindingStack.get(i + 1)).startsWith("#"))
						right = Double.valueOf(((String)bindingStack.get(i + 1))
								.substring(1));
					else {
						if ((Integer)bindingStack.get(i + 1) - 1 >= contents.length)
							return result;
						Object o = contents[(Integer)bindingStack.get(i + 1) - 1];
						if(o instanceof Number){
							right = ((Number) o).doubleValue();
						}else{
							right = Double.valueOf(o.toString());
						}
					}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_PLUS)
					{
						left += right;
						continue;
					}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_MINUS)
					{
						left -= right;
						continue;
					}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_RIDE)
					{
						left = left * right;
						continue;
					}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_DIVIDE)
					{
						left = left / right;
						continue;
					}

				}

				result = left;
			} else {
				if (bindingStack.get(0) instanceof String && ((String)bindingStack.get(0)).startsWith("#"))
					result = Double.valueOf(((String)bindingStack.get(0)).substring(1));
				else {
					if ((Integer)bindingStack.get(0) - 1 >= contents.length)
						return result;

					result = contents[(Integer)bindingStack.get(0) - 1];
				}

			}

		}

		return result;
	
	
	}
	
}
