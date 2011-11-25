/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

/**
 * 报表列别名定义，便于修改列
 * 
 * @author fangweng
 * 
 */
public class Alias implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5309011703476207347L;
	private String name;
	private String key;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

}
