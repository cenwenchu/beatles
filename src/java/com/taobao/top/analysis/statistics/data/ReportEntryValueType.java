package com.taobao.top.analysis.statistics.data;

/**
 * 报表Entry统计模式，支持最小，最大，总和，计数，平均，直接显示
 * 
 * @author fangweng
 * 
 */
public enum ReportEntryValueType {

	MIN("min"), MAX("max"), SUM("sum"), COUNT("count"), AVERAGE("average"), PLAIN(
			"plain");

	private String v;

	ReportEntryValueType(String value) {
		this.v = value;
	}

	public static ReportEntryValueType getType(String value) {
		if (value.equalsIgnoreCase("min"))
			return MIN;

		if (value.equalsIgnoreCase("max"))
			return MAX;

		if (value.equalsIgnoreCase("sum"))
			return SUM;

		if (value.equalsIgnoreCase("count"))
			return COUNT;

		if (value.equalsIgnoreCase("average"))
			return AVERAGE;

		if (value.equalsIgnoreCase("plain"))
			return PLAIN;

		throw new java.lang.RuntimeException(new StringBuilder(
				"no such ReportEntryValueType: ").append(value).toString());

	}

	@Override
	public String toString() {
		return v;
	}

}
