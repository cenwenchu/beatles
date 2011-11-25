/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * 内部key映射，用于可逆压缩
 * @author fangweng
 *
 */
public class InnerKey implements java.io.Serializable{

	private static final Log logger = LogFactory.getLog(InnerKey.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 5343510577689025678L;
	
	private String key;
	private String file;
	private Map<String,String> innerKeys;
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getFile() {
		return file;
	}
	
	public boolean setFile(String file) {
		this.file = file;
		
		if (file == null || "".equals(file))
			return false;
		
		boolean result = false;
		
		innerKeys = new HashMap<String,String>();
		
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		
		BufferedReader br = null;
		
		try
		{
			if (file.startsWith("file:"))
			{
				br = new BufferedReader(new FileReader(new File(file.substring(5))));
			}
			else
			{
				URL url = loader.getResource(file);
	
				if (url == null) {
					String error = "innerKeyFile: " + file + " not exist !";
					logger.error(error);
					throw new java.lang.RuntimeException(error);
				}
	
				br = new BufferedReader(new java.io.InputStreamReader(url.openStream()));
			}
			
			if (br != null)
			{
				String content;
				int i = 0;
				
				while((content = br.readLine()) != null)
				{
					innerKeys.put(content,new StringBuilder(AnalysisConstants.REPLACE_PREFIX)
									.append(key).append(":").append(i).toString());
					i += 1;
				}
				
				result = true;
			}
		}
		catch(Exception ex)
		{
			logger.error(ex);
		}
		finally
		{
			if (br != null)
			{
				try {
					br.close();
				} 
				catch (IOException e) {
					logger.error(e);
				}
			}
		}
		
		return result;
		
	}
	public Map<String, String> getInnerKeys() {
		return innerKeys;
	}
	public void setInnerKeys(Map<String, String> innerKeys) {
		this.innerKeys = innerKeys;
	} 

}
