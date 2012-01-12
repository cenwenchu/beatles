package com.taobao.top.analysis.statistics.data;

import java.util.List;

/**
 * 报表定义
 * 
 * @author fangweng
 * 
 */
public class Report implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 278466226057587334L;

	private String id;
	private String file;// 保存的文件名称
	private List<ReportEntry> reportEntrys;// entry的列表
	private String orderby;// 暂时未使用
	private int rowCount = 0;// 最多获取多少行
	
	private int weight = 1;//权重，用来判断结果数据量有多少，越大表示数据越多，在多个master做合并的时候会有用

	private boolean period = false;// 是否周期性输出结果，用于片段维度统计
	private long exportInterval;
	private long lastExportTime;
	private boolean append=false; 
	
	//如果内部的entry没有设置key，则使用report默认的key
	//如果设置了condition，则默认与其他的condition为与的关系
	private String key;
	private String condition;
	
	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public boolean isPeriod() {
		return period;
	}

	public void setPeriod(boolean period) {
		this.period = period;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	public String getOrderby() {
		return orderby;
	}

	public void setOrderby(String orderby) {
		this.orderby = orderby;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public List<ReportEntry> getReportEntrys() {
		return reportEntrys;
	}

	public void setReportEntrys(List<ReportEntry> reportEntrys) {
		this.reportEntrys = reportEntrys;
	}

	public final long getExportInterval() {
		return exportInterval*1000;
	}

	public final void setExportInterval(long exportInterval) {
		this.exportInterval = exportInterval;
	}

	public final long getLastExportTime() {
		return lastExportTime;
	}

	public final void setLastExportTime(long lastExportTime) {
		this.lastExportTime = lastExportTime;
	}

	public final boolean isAppend() {
		return append;
	}

	public final void setAppend(boolean append) {
		this.append = append;
	}

	
	
}
