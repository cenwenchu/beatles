/**
 * 
 */
package com.taobao.top.analysis.node;

import com.taobao.top.analysis.config.IConfig;
import com.taobao.top.analysis.exception.AnalysisException;

/**
 * 分析器基础组件接口
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public interface IComponent<C extends IConfig> {
	
	/**
	 * 初始化节点，申请资源
	 */
	public void init() throws AnalysisException;
	
	/**
	 * 节点停止的时候释放资源
	 */
	public void releaseResource();
	
	/**
	 * 获取组件的配置
	 * @return
	 */
	public C getConfig();

	/**
	 * 设置组件的配置
	 * @param config
	 */
	public void setConfig(C config);

}
