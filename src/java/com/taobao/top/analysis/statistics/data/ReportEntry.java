/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.statistics.map.IMapper;
import com.taobao.top.analysis.statistics.reduce.IReducer;
import com.taobao.top.analysis.statistics.reduce.group.GroupFunction;
import com.taobao.top.analysis.util.AnalysisConstants;

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
	private IMapper mapClass;
	
	private int[] keys;
	
	/**
	 * 用于存储一些key为对象的情况，例如某一列是json对象
	 */
	private List<ObjectColumn> subKeys;
	
	private ICalculator calculator;
	
	private ICondition condition;
	
	private IFilter valueFilter;
	/**
	 * 自定义合并方式
	 */
	private IReducer reduceClass;
	
	private GroupFunction groupFunction;
	/**
	 * mapClass带入的参数
	 */
	private String mapParams;
	/**
	 * reduceClass带入的参数
	 */
	private String reduceParams;
	
	private Map<String,Object> additions;
	
	private boolean period;
	
	/**
	 * 该entry所属的report
	 */
	private List<String> reports = new ArrayList<String>();	
	
	
	public Map<String,Object> getAdditions() {
		return additions;
	}
	public void setAdditions(String additions) {
		
		if (additions != null)
		{
			this.additions = new HashMap<String,Object>();
			
			String[] as = additions.split(",");
			
			for(String a : as)
			{
				String[] kv = a.split("=");
				
				if (kv.length == 2)
				{
					//性能考虑，这里写的比较恶劣一点，直接转换
					if (kv[0].equals(AnalysisConstants.ANALYSIS_BLOOM_MAXKEYS))
						this.additions.put(kv[0], Integer.parseInt(kv[1]));
					else
						if (kv[0].equals(AnalysisConstants.ANALYSIS_BLOOM_ERRORRATE))
							this.additions.put(kv[0], Float.parseFloat(kv[1]));
						else
							this.additions.put(kv[0], kv[1]);
				}
			}
			
		}
			
	}
	public List<String> getReports() {
		return reports;
	}
	public void setReports(List<String> reports) {
		this.reports = reports;
	}
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
	
	public IMapper getMapClass() {
		return mapClass;
	}
	public void setMapClass(IMapper mapClass) {
		this.mapClass = mapClass;
		if(mapClass instanceof com.taobao.top.analysis.statistics.map.TimeKeyMapper) {
		    this.period = true;
		}
	}
	public IReducer getReduceClass() {
		return reduceClass;
	}
	public void setReduceClass(IReducer reduceClass) {
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
	public ICalculator getCalculator() {
		return calculator;
	}
	public void setCalculator(ICalculator calculator) {
		this.calculator = calculator;
	}
	public ICondition getCondition() {
		return condition;
	}
	public void setCondition(ICondition condition) {
		this.condition = condition;
	}
	public IFilter getValueFilter() {
		return valueFilter;
	}
	public void setValueFilter(IFilter valueFilter) {
		this.valueFilter = valueFilter;
	}
	public int[] getKeys() {
		return keys;
	}
	public void setKeys(int[] keys) {
		this.keys = keys;
	}
	public List<ObjectColumn> getSubKeys() {
		return subKeys;
	}
	public void setSubKeys(List<ObjectColumn> subKeys) {
		this.subKeys = subKeys;
	}
	public GroupFunction getGroupFunction() {
		return groupFunction;
	}
	public void setGroupFunction(GroupFunction groupFunction) {
		this.groupFunction = groupFunction;
	}
	@Override
	public ReportEntry clone() throws CloneNotSupportedException {
		return (ReportEntry) super.clone();
	}
    /**
     * @return the period
     */
    public boolean isPeriod() {
        return period;
    }
    /**
     * @param period the period to set
     */
    public void setPeriod(boolean period) {
        this.period = period;
    }
	
	
	

}
