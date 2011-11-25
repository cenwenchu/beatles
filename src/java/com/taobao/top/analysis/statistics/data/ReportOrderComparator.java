package com.taobao.top.analysis.statistics.data;

import java.util.Comparator;

/**
 * @author fangweng
 * 
 */
public class ReportOrderComparator<T> implements Comparator<T> {
	private int[] column;
	private boolean[] isDesc;

	public ReportOrderComparator(int[] column, boolean[] isDesc) {
		this.column = column;
		this.isDesc = isDesc;
	}

	@Override
	public int compare(T o1, T o2) {
		Object[] or1 = (Object[]) o1;
		Object[] or2 = (Object[]) o2;
		int compareValue = 0;

		for (int i = 0; i < column.length; i++) {
			if (or1[column[i]] instanceof Double) {
				compareValue = (int) ((Double) or1[column[i]] - (Double) or2[column[i]]);
			} else
				compareValue = String.valueOf(or1[column[i]]).compareTo(
						String.valueOf(or2[column[i]]));
			if (compareValue != 0) {
				if (isDesc[i])
					return -compareValue;
				else
					return compareValue;
			}
		}

		return compareValue;
	}

}
