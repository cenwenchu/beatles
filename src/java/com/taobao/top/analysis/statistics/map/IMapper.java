package com.taobao.top.analysis.statistics.map;

import java.io.Serializable;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
/**
 * 
 * @author zhudi
 *mapper操作，完成从一行数据中产生对  key-value对
 * @param <T>
 */
public interface IMapper extends Cloneable,Serializable{
	
	/**
	 * 生成key
	 * @param entry
	 * @param content
	 * @param jobtask
	 * @return
	 */
	public String mapperKey(ReportEntry entry,String[] content,JobTask jobtask);
	/**
	 * 生成value
	 * @param entry
	 * @param content
	 * @param jobtask
	 * @return
	 */
	public Object mapperValue(ReportEntry entry,Object[] content,JobTask jobtask);

}
