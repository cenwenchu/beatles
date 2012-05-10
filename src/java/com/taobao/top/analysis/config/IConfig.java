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
	
	/**
	 * 从配置中根据名称获得属性内容
	 * @param propName
	 */
	public String get(String propName);
	
	/**
	 * 判断是否需要从外部重新加载配置
	 * 此处暂实现判断配置文件的变更
	 * @return
	 */
	public boolean isNeedReload();
	
	/**
	 * 重新载入配置文件
	 */
	public void reload();
	
	/**
	 * 将config中需要序列化的内容序列化成为字符串
	 * @return
	 */
	public String marshal();
	
	/**
	 * 从content中反解出内容设置到config中
	 * @param content
	 * @return
	 */
	public void unmarshal(String content);
	
}
