/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;

import com.taobao.top.analysis.statistics.map.IMapper;
import com.taobao.top.analysis.statistics.reduce.IReducer;

/**
 * SimpleMapReduce Entry
 * 
 * @author fangweng
 * 
 */
public class ReportEntry implements Serializable, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6275004376213449537L;

	private String id;
	
	/**
	 * 显示在报表的列title
	 */
	private String name;
	
	/**
	 * 是否是后处理的列，例如有些列需要由某几个统计列再次作处理， 因此必须在这些列处理以后再统计
	 */
	private boolean lazy;
	
	/**
	 * 自定义key生成方法
	 */
	private IMapper<ReportEntry> mapClass;
	/**
	 * 自定义value生成方法
	 */
	private IReducer<ReportEntry> reduceClass;
	/**
	 * mapClass带入的参数
	 */
	private String mapParams;
	/**
	 * reduceClass带入的参数
	 */
	private String reduceParams;
	
	public boolean isLazy() {
		return lazy;
	}
	public void setLazy(boolean lazy) {
		this.lazy = lazy;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public IMapper<ReportEntry> getMapClass() {
		return mapClass;
	}
	public void setMapClass(IMapper<ReportEntry> mapClass) {
		this.mapClass = mapClass;
	}
	public IReducer<ReportEntry> getReduceClass() {
		return reduceClass;
	}
	public void setReduceClass(IReducer<ReportEntry> reduceClass) {
		this.reduceClass = reduceClass;
	}
	public String getMapParams() {
		return mapParams;
	}
	public void setMapParams(String mapParams) {
		this.mapParams = mapParams;
	}
	public String getReduceParams() {
		return reduceParams;
	}
	public void setReduceParams(String reduceParams) {
		this.reduceParams = reduceParams;
	}
	@Override
	public ReportEntry clone() throws CloneNotSupportedException {
		return (ReportEntry) super.clone();
	}
	
	
	

}
