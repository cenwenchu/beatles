/**
 * 
 */
package com.taobao.top.analysis.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 配置抽象类
 * 参数采取动态获取，每次获取都会可能产生消耗，注意使用场景
 * @author fangweng
 *
 */
public abstract class AbstractConfig implements IConfig{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1038082086935557144L;
	
	Map<String,String> properties = new HashMap<String,String>();
	
	public void addAllToConfig(Map<String,String> props)
	{
		this.properties.putAll(props);
	}
	
	public String toString()
	{
		StringBuilder st = new StringBuilder();
		
		Iterator<String> keys = properties.keySet().iterator();
		
		while(keys.hasNext())
		{
			String key = keys.next();
			
			st.append(key).append("=").append(properties.get(key)).append(" ,");
		}
		
		return st.toString();
	}

}
