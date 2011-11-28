/**
 * 
 */
package com.taobao.top.analysis.node;


import java.util.List;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.job.Job;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobBuilder extends IComponent<MasterConfig>{
	
	public List<Job> build(String config) throws AnalysisException;

}
