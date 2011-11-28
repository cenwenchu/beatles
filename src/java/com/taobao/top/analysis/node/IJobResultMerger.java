/**
 * 
 */
package com.taobao.top.analysis.node;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.job.Job;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobResultMerger extends IComponent<MasterConfig>{
	
	public void merge(Job job);

}
