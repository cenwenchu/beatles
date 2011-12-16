package com.taobao.top.analysis.statistics.data;

import com.taobao.top.analysis.statistics.reduce.group.GroupFunction;

/**
 * 
 * @author zhudi
 *
 */
public class ExpressionReportEntry extends ReportEntry {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6629045500102888849L;
	
	/**
	 * 统计的主键列描述
	 */
	private int[] keys;
	
	private String condition;
	
	private String filter;
	
	private String value;
	
	private GroupFunction groupFunction;
	
	public GroupFunction getGroupFunction() {
		return groupFunction;
	}

	public void setGroupFunction(GroupFunction groupFunction) {
		this.groupFunction = groupFunction;
	}

	public int[] getKeys() {
		return keys;
	}

	public void setKeys(int[] keys) {
		this.keys = keys;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}
	
	
	
	
}
