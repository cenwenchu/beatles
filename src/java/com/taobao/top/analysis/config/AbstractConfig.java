/**
 * 
 */
package com.taobao.top.analysis.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.util.ReportUtil;

/**
 * 配置抽象类
 * 参数采取动态获取，每次获取都会可能产生消耗，注意使用场景
 * @author fangweng
 *
 */
public abstract class AbstractConfig implements IConfig{
	
	private static final Log logger = LogFactory.getLog(AbstractConfig.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1038082086935557144L;
	
	/**
	 * 用于存储动态属性的map
	 */
	Map<String,String> properties = new HashMap<String,String>();
	
	/**
	 * 从配置中根据名称获得属性内容
	 * @param propName
	 */
	@Override
	public String get(String propName)
	{
		return this.properties.get(propName);
	}
	
	@Override
	public void addAllToConfig(Map<String,String> props)
	{
		this.properties.putAll(props);
	}
	
	/**
	 * 从外部配置文件载入
	 * @param file
	 */
	@Override
	public void load(String file)
	{
		InputStream in = null;
		
		try
		{
			in = ReportUtil.getInputStreamFromFile(file);
			
			Properties prop = new Properties();
			prop.load(in);
			
			Iterator<Object> keys = prop.keySet().iterator();
			
			while(keys.hasNext())
			{
				String key = (String)keys.next();
				
				properties.put(key, prop.getProperty(key));
			}
			
		}
		catch(Exception ex)
		{
			logger.error(ex,ex);
		}
		finally
		{
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error(e,e);
				}
		}
		
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
