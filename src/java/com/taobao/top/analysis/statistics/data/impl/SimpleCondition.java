package com.taobao.top.analysis.statistics.data.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.ICondition;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;

public class SimpleCondition implements ICondition {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -684511768155047811L;
	/**
	 * condition的key保留队列
	 */
	List<Object> conditionKStack;
	/**
	 * condition的value保留队列
	 */
	List<Object> conditionVStack;
	/**
	 * condition的操作保留队列
	 */
	List<Byte> conditionOpStack;
	
	/**
	 * 条件之间是否是与的关系，当前只支持全部是与或者全部是或。
	 */
	private boolean andCondition = true;
	
	private String condition;
	
	public String getCondition() {
		return condition;
	}
	
	public void init(Map<String, Alias> aliasPool) throws AnalysisException{
		if (condition != null && !"".equals(condition)) {
			conditionKStack = new ArrayList<Object>();
			conditionVStack = new ArrayList<Object>();
			conditionOpStack = new ArrayList<Byte>();

			String[] cons;

			if (condition.indexOf("||") > 0) {
				cons = StringUtils.split(condition, "||");
				this.andCondition = false;
			} else {
				cons = StringUtils.split(condition, "&");
				this.andCondition = true;
			}

			for (String con : cons) {
				con = con.trim();
				if (!con.startsWith("$"))
					continue;

				Object key = ReportUtil.transformVar(
						con.substring(1, con.lastIndexOf("$")), aliasPool);
				String value = con.substring(con.lastIndexOf("$") + 1).trim();
				String operate = null;

				if (value.startsWith(AnalysisConstants.CONDITION_NOT_EQUAL_STR)
						|| value.startsWith(AnalysisConstants.CONDITION_EQUALORGREATER_STR)
						|| value.startsWith(AnalysisConstants.CONDITION_EQUALORLESSER_STR)
						|| value.startsWith(AnalysisConstants.CONDITION_IN_STR)) {
					operate = value.substring(0, 2);
					value = value.substring(2).trim();
				} else if(value.startsWith(AnalysisConstants.CONDITION_LIKE_STR)) {
				    operate = value.substring(0, 4);
				    value = value.substring(4).trim();
				} else {
					if (value.startsWith(AnalysisConstants.CONDITION_EQUAL_STR)
							|| value.startsWith(AnalysisConstants.CONDITION_LESSER_STR)
							|| value.startsWith(AnalysisConstants.CONDITION_GREATER_STR)) {
						operate = value.substring(0, 1);
						value = value.substring(1).trim();
					}
				}

				if (!key.equals(AnalysisConstants.RECORD_LENGTH))
				{
				    if(!(key instanceof Integer))
				        conditionKStack.add(Integer.valueOf(String.valueOf(key)));
				    else
				        conditionKStack.add(key);
					
					if (operate.equals(AnalysisConstants.CONDITION_EQUAL_STR)
							|| operate.equals(AnalysisConstants.CONDITION_NOT_EQUAL_STR)
							|| operate.equals(AnalysisConstants.CONDITION_IN_STR))
					{
						conditionVStack.add(value);
					}
					else if (operate.equals(AnalysisConstants.CONDITION_LIKE_STR)) {
					    conditionVStack.add(Pattern.compile(value));
					}
					else
						conditionVStack.add(Double.valueOf(value));
				}
				else
				{
					conditionKStack.add(key);
					conditionVStack.add(Integer.valueOf(value.trim()));
				}

				
				conditionOpStack.add(ReportUtil.generateOperationFlag(operate));
			}
		}
	}

	public SimpleCondition(String conditions, Map<String, Alias> aliasPool)throws AnalysisException {
		this.condition = conditions;
		init(aliasPool);
	}
	
	public void appendCondition(String conditions, Map<String, Alias> aliasPool)throws AnalysisException {

		if (this.condition != null && !"".equals(this.condition)) {
			this.condition += "&" + conditions;
		} else {
			this.condition = conditions;
		}
		init(aliasPool);
	}

	@Override
	public boolean isInCondition(String[] contents) {
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
	private static boolean checkKeyCondition(byte operator, int conditionKey,
			Object conditionValue, String[] contents) {
		boolean result = false;
		
		if(conditionKey > contents.length || (conditionKey >= 1 && StringUtils.isBlank(contents[conditionKey - 1]) || conditionValue == null ))
		    return result;

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
		} 
		else if (operator == AnalysisConstants.CONDITION_IN) {
			if (conditionKey > 0)
				result = (new StringBuilder().append(conditionValue).append(","))
					.indexOf(new StringBuilder().append(contents[conditionKey - 1]).append(",").toString())>=0;
		} else if(operator == AnalysisConstants.CONDITION_LIKE) {
		    if(conditionKey > 0 && conditionValue instanceof Pattern)
		        result = ((Pattern)conditionValue).matcher(contents[conditionKey - 1]).matches();
		}
		else {
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

	
	
	

}
