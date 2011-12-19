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
public interface IMapper extends Cloneable,Serializable{
	
	public String mapperKey(ReportEntry entry,String[] content,JobTask jobtask);

	public Object mapperValue(ReportEntry entry,Object[] content,JobTask jobtask);

}
