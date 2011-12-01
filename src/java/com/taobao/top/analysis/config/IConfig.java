package com.taobao.top.analysis.config;

import java.util.Map;

/**
 * 系统配置文件接口
 * @author fangweng
 *
 */
public interface IConfig extends java.io.Serializable{
	
	/**
	 * 将外部的配置加入到配置中
	 * @param 外部配置
	 */
	public void addAllToConfig(Map<String,String> props);
	
	/**
	 * 从外部配置文件载入
	 * @param properties
	 */
	public void load(String properties);
}
