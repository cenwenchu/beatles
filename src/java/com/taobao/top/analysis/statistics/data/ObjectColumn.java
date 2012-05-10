/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午2:56:06
 *
 */
public class ObjectColumn implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int cIndex;
	private String subKeyName;
	
	public ObjectColumn(int cIndex,String subKeyName)
	{
		this.cIndex = cIndex;
		this.subKeyName = subKeyName;
	}
	
	public int getcIndex() {
		return cIndex;
	}
	public void setcIndex(int cIndex) {
		this.cIndex = cIndex;
	}
	public String getSubKeyName() {
		return subKeyName;
	}
	public void setSubKeyName(String subKeyName) {
		this.subKeyName = subKeyName;
	}
	
}
