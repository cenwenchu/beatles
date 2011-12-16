package com.taobao.top.analysis.statistics.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.ExpressionReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;
/**
 * 
 * @author zhudi
 *
 */
public class DefaultExpReportEntry extends ExpressionReportEntry {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6629045500102888849L;
	
	/**
	 * 统计的主键列描述
	 */
	private int[] keys;
	
	/**
	 * condition的key保留队列
	 */
	private List<Object> conditionKStack;
	/**
	 * condition的value保留队列
	 */
	private List<Object> conditionVStack;
	/**
	 * condition的操作保留队列
	 */
	private List<Byte> conditionOpStack;
	
	/**
	 * 条件之间是否是与的关系，当前只支持全部是与或者全部是或。
	 */
	private boolean andCondition = true;
	
	/**
	 * value表达式中的变量列表
	 */
	private List<Object> bindingStack;
	/**
	 * value表达式中的操作列表
	 */
	private List<Byte> operatorStack;
	
	/**
	 * 值过滤条件
	 */
	private List<Object> valuefilterStack;
	/**
	 * 值过滤条件中的操作符列表
	 */
	private List<Byte> valuefilterOpStack;
	
	/**
	 * 是否需要格式化结果，在报表生成的时候格式化，当前支持round
	 */
	private List<String> formatStack;
	
	
	public void setFilter(String valuefilter,Map<String, Alias> aliasPool) throws AnalysisException{
		super.setFilter(valuefilter);
		if (valuefilter != null && !"".equals(valuefilter)) {
			String[] filters = StringUtils.split(valuefilter, "&");

			valuefilterStack = new ArrayList<Object>();
			valuefilterOpStack = new ArrayList<Byte>();

			formatStack = new ArrayList<String>();

			for (String f : filters) {
				if (f.startsWith(AnalysisConstants.CONDITION_ROUND_STR))
					formatStack.add(f);
				else {
					if (f.startsWith(AnalysisConstants.CONDITION_ISNUMBER_STR)) {
						valuefilterOpStack.add(ReportUtil.generateOperationFlag(f));
						valuefilterStack.add(f);
					} else {

						if (f.startsWith(AnalysisConstants.CONDITION_EQUALORGREATER_STR)
								|| f.startsWith(AnalysisConstants.CONDITION_EQUALORLESSER_STR)
								|| f.startsWith(AnalysisConstants.CONDITION_NOT_EQUAL_STR)) {
							valuefilterOpStack.add(ReportUtil.generateOperationFlag(f.substring(0, 2)));
							
							if (f.startsWith(AnalysisConstants.CONDITION_NOT_EQUAL_STR))
								valuefilterStack.add(f.substring(2));
							else
								valuefilterStack.add(Double.valueOf(f.substring(2)));
							
						} else {
							if (f.startsWith(AnalysisConstants.CONDITION_EQUAL_STR)
									|| f.startsWith(AnalysisConstants.CONDITION_GREATER_STR)
									|| f.startsWith(AnalysisConstants.CONDITION_LESSER_STR)) {
								valuefilterOpStack.add(ReportUtil.generateOperationFlag(f.substring(0, 1)));
								
								if (f.startsWith(AnalysisConstants.CONDITION_EQUAL_STR))
									valuefilterStack.add(f.substring(1));
								else
									valuefilterStack.add(Double.valueOf(f.substring(1)));
							}
						}
					}
				}
			}

		}else{
			valuefilterStack = null;
			valuefilterOpStack = null;
			formatStack = null;
		}
	
	}



	public void setCondition(String condition,Map<String, Alias> aliasPool) throws AnalysisException {
		super.setCondition(condition);

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
				if (!con.startsWith("$"))
					continue;

				Object key = ReportUtil.transformVar(
						con.substring(1, con.lastIndexOf("$")), aliasPool);
				String value = con.substring(con.lastIndexOf("$") + 1);
				String operate = null;

				if (value.startsWith(AnalysisConstants.CONDITION_NOT_EQUAL_STR)
						|| value.startsWith(AnalysisConstants.CONDITION_EQUALORGREATER_STR)
						|| value.startsWith(AnalysisConstants.CONDITION_EQUALORLESSER_STR)) {
					operate = value.substring(0, 2);
					value = value.substring(2);
				} else {
					if (value.startsWith(AnalysisConstants.CONDITION_EQUAL_STR)
							|| value.startsWith(AnalysisConstants.CONDITION_LESSER_STR)
							|| value.startsWith(AnalysisConstants.CONDITION_GREATER_STR)) {
						operate = value.substring(0, 1);
						value = value.substring(1);
					}
				}

				if (!key.equals(AnalysisConstants.RECORD_LENGTH))
				{
					conditionKStack.add(key);
					
					if (operate.equals(AnalysisConstants.CONDITION_EQUAL_STR)
							|| operate.equals(AnalysisConstants.CONDITION_NOT_EQUAL_STR))
					{
						conditionVStack.add(value);
					}
					else
						conditionVStack.add(Double.valueOf(value));
				}
				else
				{
					conditionKStack.add(key);
					conditionVStack.add(Integer.valueOf(value));
				}

				
				conditionOpStack.add(ReportUtil.generateOperationFlag(operate));
			}
		}
	
	}



	public void setValue(String value,Map<String, Alias> aliasPool)  throws AnalysisException{
		super.setValue(value);
		if (value != null
				&& !"".equals(value)
				&& (value.indexOf("$") >= 0 || value
						.indexOf("entry(") >= 0)) {

			bindingStack = new ArrayList<Object>();
			operatorStack = new ArrayList<Byte>();

			String c = value;
			String temp;

			while (c.indexOf("$") >= 0 || c.indexOf("#") >= 0) {
				if (c.indexOf("$") >= 0) {
					if (c.indexOf("#") < 0
							|| (c.indexOf("#") >= 0 && c.indexOf("$") < c
									.indexOf("#"))) {
						c = c.substring(c.indexOf("$") + 1);
						temp = c.substring(0, c.indexOf("$"));
						c = c.substring(c.indexOf("$") + 1);

						if (aliasPool != null && aliasPool.size() > 0
								&& aliasPool.get(temp) != null) {
							bindingStack.add(aliasPool.get(temp).getKey());
						} else
							bindingStack.add(temp);

						continue;
					}
				}

				if (c.indexOf("#") >= 0) {
					if (c.indexOf("$") < 0
							|| (c.indexOf("$") >= 0 && c.indexOf("$") > c
									.indexOf("#"))) {
						c = c.substring(c.indexOf("#") + 1);
						temp = c.substring(0, c.indexOf("#"));
						c = c.substring(c.indexOf("#") + 1);

						bindingStack.add("#" + temp);

						continue;
					}
				}

			}

			while (c.indexOf("entry(") >= 0) {
				c = c.substring(c.indexOf("entry(") + "entry(".length());
				temp = c.substring(0, c.indexOf(")"));
				c = c.substring(c.indexOf(")") + 1);
				bindingStack.add(temp);
			}

			char[] cs = value.toCharArray();

			for (char _ch : cs) {
				if (_ch == '+' || _ch == '-' || _ch == '*' || _ch == '/')
					operatorStack.add(ReportUtil.generateOperationFlag(_ch));
			}

		}
	
	
	}



	public List<Object> getBindingStack() {
		return bindingStack;
	}

	public List<Byte> getOperatorStack() {
		return operatorStack;
	}

//	public ICalculator getCalculator() {
//		return calculator;
//	}
//
//	public void setCalculator(ICalculator calculator) {
//		this.calculator = calculator;
//	}

//	public ICondition getCondition() {
//		return condition;
//	}
//
//	public void setCondition(ICondition condition) {
//		this.condition = condition;
//	}
//
//	public IFilter getFilter() {
//		return filter;
//	}
//
//	public void setFilter(IFilter filter) {
//		this.filter = filter;
//	}
	
	

	public int[] getKeys() {
		return keys;
	}

	public List<Object> getConditionKStack() {
		return conditionKStack;
	}



	public List<Object> getConditionVStack() {
		return conditionVStack;
	}



	public List<Byte> getConditionOpStack() {
		return conditionOpStack;
	}



	public boolean isAndCondition() {
		return andCondition;
	}


	public List<Object> getValuefilterStack() {
		return valuefilterStack;
	}



	public List<Byte> getValuefilterOpStack() {
		return valuefilterOpStack;
	}



	public List<String> getFormatStack() {
		return formatStack;
	}



	public void setKeys(int[] keys) {
		this.keys = keys;
	}
	@Override
	public DefaultExpReportEntry clone() throws CloneNotSupportedException {
		return (DefaultExpReportEntry) super.clone();
	}
	
	
}
