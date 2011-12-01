/**
 * 
 */
package com.taobao.top.analysis.node;


import java.util.Map;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.job.Job;

/**
 * 分析器Master获取任务集的管理接口
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IJobBuilder extends IComponent<MasterConfig>{
	
	/**
	 * 默认从config获得job编译路径
	 * @return
	 * @throws AnalysisException
	 */
	public Map<String,Job> build() throws AnalysisException;
	
	/**
	 * 从某一个位置获取任务集
	 * @param 可以自己扩展是从本地文件载入还是http等其他方式载入
	 * @return
	 * @throws AnalysisException
	 */
	public Map<String,Job> build(String config) throws AnalysisException;
	
	/**
	 * 重新载入
	 * @return
	 * @throws AnalysisException
	 */
	public Map<String,Job> rebuild() throws AnalysisException;
	
	/**
	 * 判断是否需要重新载入
	 * @return
	 */
	public boolean isNeedRebuild();

	/**
	 * 设置重新载入
	 * @param needRebuild
	 */
	public void setNeedRebuild(boolean needRebuild);

}
