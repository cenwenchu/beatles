package com.taobao.top.analysis.statistics.data.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.statistics.data.IFilter;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;

public class SimpleFilter implements IFilter {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7515455764712362921L;
	/**
	 * 值过滤条件
	 */
	private final List<Object> valuefilterStack;
	/**
	 * 值过滤条件中的操作符列表
	 */
	private final List<Byte> valuefilterOpStack;
	
	/**
	 * 是否需要格式化结果，在报表生成的时候格式化，当前支持round
	 */
	private final List<String> formatStack;
	
	public List<Object> getValuefilterStack() {
		return valuefilterStack;
	}

	public List<Byte> getValuefilterOpStack() {
		return valuefilterOpStack;
	}

	public List<String> getFormatStack() {
		return formatStack;
	}



	public SimpleFilter(String valuefilter)  throws AnalysisException {

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



	@Override
	public Object filter(Object value) {


		if (valuefilterStack == null
				|| (valuefilterStack != null && valuefilterStack.size() == 0))
			return value;

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
						return null;
				}

				if (filterOpt == AnalysisConstants.CONDITION_EQUALORGREATER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v >= compareValue) {
						continue;
					} else
						return null;
				}

				if (filterOpt == AnalysisConstants.CONDITION_EQUALORLESSER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v <= compareValue) {
						continue;
					} else
						return null;
				}

				if (filterOpt == AnalysisConstants.CONDITION_GREATER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v > compareValue) {
						continue;
					} else
						return null;
				}

				if (filterOpt == AnalysisConstants.CONDITION_LESSER) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = (Double)filterValue;

					if (v < compareValue) {
						continue;
					} else
						return null;
				}

				if (filterOpt == AnalysisConstants.CONDITION_NOT_EQUAL) {
					if (!value.equals(filterValue)) {
						continue;
					} else
						return null;
				}

			}
		} catch (Exception ex) {
			return null;
		}

		return value;
	
	}

}
