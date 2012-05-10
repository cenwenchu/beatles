/**
 * 
 */
package com.taobao.top.analysis.util.bloom;


/**
 * 
 * bloom过滤器接口定义
 * @author fangweng
 * @email fangweng@taobao.com
 * @date 2011-3-16
 *
 */
public interface BloomFilter {
	
	void setHash(Hash hash);
	
	/**
	 * Allocate memory for the bloom filter data. Note that bloom data isn&apos;
	 * t allocated by default because it can grow large & reads would be better managed by the LRU cache.
	 */
	void allocBloom();
	
	/**
	 * Add the specified binary to the bloom filter.
	 * @param buf data to be added to the bloom
	 */
	void add(byte []buf);
	
	/**
	 * Add the specified binary to the bloom filter. 
	 * @param buf data to be added to the bloom
	 * @param offset offset into the data to be added
	 * @param len length of the data to be added
	 */
	void add(byte []buf, int offset, int len);
	
	/**
	 * Check if the specified key is contained in the bloom filter.
	 * 
	 * @param buf data to check for existence of
	 * @return true if matched by bloom, false if not
	 */
	boolean contains(byte [] buf);
	
	/**
	 * Check if the specified key is contained in the bloom filter.
	 * 
	 * @param buf data to check for existence of
	 * @param offset offset into the data
	 * @param length length of the data
	 * @return true if matched by bloom, false if not
	 */
	boolean contains(byte [] buf, int offset, int length);
	
	/**
	 * The number of keys added to the bloom
	 * @return
	 */
	int getKeyCount();
	
	/**
	 * The max number of keys that can be inserted to maintain the desired error rate
	 * @return
	 */
	public int getMaxKeys();
	
	/**
	 * Size of the bloom, in bytes
	 * @return
	 */
	public int getByteSize();
	
	/**
	 * Compact the bloom before writing metadata & data to disk
	 */
	void compactBloom();
	
	/**
	 * Get a writable interface into bloom filter meta data. 
	 * @return
	 */
	Writable getMetaWriter();
	
	/**
	 * Get a writable interface into bloom filter data (actual bloom). 
	 * @return
	 */
	Writable getDataWriter();
	
	
}
