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
public class MurmurHash implements Hash{
	private static MurmurHash _instance = new MurmurHash();

	public static MurmurHash getInstance() {
		return _instance;
	}

	public int hash(byte[] data, int offset, int length, int seed) {
		int m = 0x5bd1e995;
		int r = 24;
		int h = seed ^ length;
		
		int len_4 = length >> 2;
		for (int i = 0; i < len_4; i++) {
			int i_4 = (i << 2) + offset;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			
			// murmur mix function
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}
		
		
		
		int len_m = len_4 << 2;
		int left = length - len_m;
		int i_m = len_m + offset;
		if (left != 0) {
			if (left >= 3) {
				h ^= data[i_m + 2] << 16;
			}
			if (left >= 2) {
				h ^= data[i_m + 1] << 8;
			}
			if (left >= 1) {
				h ^= data[i_m];
			}
			h *= m;
		}
		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;
			
		return h;
	}

}
