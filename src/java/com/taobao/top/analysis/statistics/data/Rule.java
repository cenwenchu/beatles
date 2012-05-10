package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 配置信息的传输对象，用于master向slave传输配置信息
 * 
 * @author fangweng
 * 
 */
public class Rule implements Serializable {

	private static final long serialVersionUID = -4977691745701083747L;

	/**
	 * entry定义池
	 */
	private Map<String, ReportEntry> entryPool;
	/**
	 * 报表定义池
	 */
	private Map<String, Report> reportPool;
	
	/**
	 * 支持多个master来分担合并数据的任务，这里保存的是报表和master的对应关系
	 * reportid,masterip:port
	 */
	private Map<String, String> report2Master;
	/**
	 * 别名定义池
	 */
	private Map<String, Alias> aliasPool;

	/**
	 * 父亲entry定义池
	 */
	Map<String, ReportEntry> parentEntryPool;

	/**
	 * 记录所有被引用的Entry的定义，用于过滤没有被引用的entry定义
	 */
	private Map<String, ReportEntry> referEntrys;

	/**
	 * 配置信息的版本号 格式为TimeInMillis值
	 */
	private long version;

	/**
	 * 所属域
	 */
	private String domain;
	
	//用于将某些字段替换成短标识放入到key中，节省计算的内存
	private List<InnerKey> innerKeyPool;
	
	/**
     * 目前集群中所有的master
     */
    private Set<String> masters;

	public Rule() {
		entryPool = new HashMap<String, ReportEntry>();
		parentEntryPool = new HashMap<String, ReportEntry>();
		reportPool = new TreeMap<String, Report>();
		report2Master = new HashMap<String,String>();
		aliasPool = new HashMap<String, Alias>();
		referEntrys = new HashMap<String, ReportEntry>();
		innerKeyPool = new ArrayList<InnerKey>();
		version = System.currentTimeMillis();
	}

	public void clear() {
		entryPool.clear();
		parentEntryPool.clear();
		reportPool.clear();
		aliasPool.clear();
		referEntrys.clear();
		innerKeyPool.clear();
		report2Master.clear();

		domain = null;
		version = 0;
	}

	public Map<String, String> getReport2Master() {
		return report2Master;
	}

	public void setReport2Master(Map<String, String> report2Master) {
		this.report2Master = report2Master;
	}

	public List<InnerKey> getInnerKeyPool() {
		return innerKeyPool;
	}

	public void setInnerKeyPool(List<InnerKey> innerKeyPool) {
		this.innerKeyPool = innerKeyPool;
	}

	public String getDomain() {
		return domain;
	}

	public Map<String, ReportEntry> getReferEntrys() {
		return referEntrys;
	}

	public void setReferEntrys(Map<String, ReportEntry> referEntrys) {
		this.referEntrys = referEntrys;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public Map<String, ReportEntry> getParentEntryPool() {
		return parentEntryPool;
	}

	public void setParentEntryPool(Map<String, ReportEntry> parentEntryPool) {
		this.parentEntryPool = parentEntryPool;
	}

	public Map<String, ReportEntry> getEntryPool() {
		return entryPool;
	}

	public void setEntryPool(Map<String, ReportEntry> entryPool) {
		this.entryPool = entryPool;
	}

	public Map<String, Report> getReportPool() {
		return reportPool;
	}

	public void setReportPool(Map<String, Report> reportPool) {
		this.reportPool = reportPool;
	}

	public Map<String, Alias> getAliasPool() {
		return aliasPool;
	}

	public void setAliasPool(Map<String, Alias> aliasPool) {
		this.aliasPool = aliasPool;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

    /**
     * @return the masters
     */
    public Set<String> getMasters() {
        return masters;
    }

    /**
     * @param masters the masters to set
     */
    public void setMasters(Set<String> masters) {
        this.masters = masters;
    }

}
