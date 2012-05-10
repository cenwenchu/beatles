package com.taobao.top.analysis.statistics.reduce.group;

/**
 * 
 * 组方法工厂
 * 报表Entry统计模式，支持最小，最大，总和，计数，平均，直接显示,去重计数
 * 
 * @author fangweng
 * 
 */
public class GroupFunctionFactory {
	
	private static final GroupFunction MIN = new MinFunction();
	private static final GroupFunction MAX = new MaxFunction();
	private static final GroupFunction SUM = new SumFunction();
	private static final GroupFunction COUNT = new CountFunction();
	private static final GroupFunction AVERAGE = new AvgFunction();
	private static final GroupFunction PLAIN = new PlainFunction();
	private static final GroupFunction DISTINCTCOUNT = new DistinctCountFunction();

	public static GroupFunction getFunction(String value) {
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
		
		if (value.equalsIgnoreCase("dcount"))
			return DISTINCTCOUNT;

		throw new java.lang.RuntimeException(new StringBuilder(
				"no such ReportEntryValueType: ").append(value).toString());

	}

}
