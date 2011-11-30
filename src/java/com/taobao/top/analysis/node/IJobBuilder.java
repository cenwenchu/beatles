/**
 * 
 */
package com.taobao.top.analysis.node;


import java.util.Map;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.job.Job;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobBuilder extends IComponent<MasterConfig>{
	
	public Map<String,Job> build(String config) throws AnalysisException;
	
	public Map<String,Job> rebuild() throws AnalysisException;
	
	public boolean isNeedRebuild();

	public void setNeedRebuild(boolean needRebuild);

}
