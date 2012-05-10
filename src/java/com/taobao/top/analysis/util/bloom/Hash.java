/**
 * 
 */
package com.taobao.top.analysis.util.bloom;

/**
 * @author fangweng
 * @email fangweng@taobao.com
 * @date 2011-3-16
 *
 */
public interface Hash {

	public int hash(byte[] data, int offset, int length, int seed);
}
