package com.taobao.top.analysis.statistics.map;

import java.io.Serializable;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
/**
 * 
 * @author zhudi
 *
 * @param <T>
 */
public interface IMapper<T extends ReportEntry> extends Cloneable,Serializable{
	
	public String mapperKey(T entry,String[] content,JobTask jobtask);
	
	
	public Object mapperValue(T entry,Object[] content,JobTask jobtask);

}
