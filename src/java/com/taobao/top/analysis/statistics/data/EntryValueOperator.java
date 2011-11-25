/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

/**
 * Entry value的操作符定义
 * 
 * @author fangweng
 * 
 */
public enum EntryValueOperator {

	PLUS("+"), MINUS("-"), RIDE("*"), DIVIDE("/");

	private String v;

	EntryValueOperator(String value) {
		v = value;
	}

	@Override
	public String toString() {
		return v;
	}

}
