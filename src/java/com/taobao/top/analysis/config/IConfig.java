package com.taobao.top.analysis.config;

import java.util.Map;

/**
 * 系统配置文件接口
 * @author fangweng
 *
 */
public interface IConfig extends java.io.Serializable{
	
	public void addAllToConfig(Map<String,String> props);
}
