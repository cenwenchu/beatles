/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.top.analysis.util.bloom.ByteBloomFilter;

/**
 * @author fangweng
 * @email fangweng@taobao.com
 * @date 2011-3-16
 *
 */
public class DistinctCountEntryValue {
	private AtomicLong count = new AtomicLong(0);
	private ByteBloomFilter bloomFilter;
	
	public AtomicLong getCount() {
		return count;
	}
	public void setCount(AtomicLong count) {
		this.count = count;
	}
	public ByteBloomFilter getBloomFilter() {
		return bloomFilter;
	}
	public void setBloomFilter(ByteBloomFilter bloomFilter) {
		this.bloomFilter = bloomFilter;
	}
	
	public void add(String key) throws UnsupportedEncodingException
	{
		if (key == null || "".equals(key))
			return;
			
		byte[] k = key.getBytes("UTF-8");
		
		if (!bloomFilter.contains(k))
		{
			bloomFilter.add(k);
			count.incrementAndGet();
		}
	}
	
	public void add(byte[] key) throws UnsupportedEncodingException
	{
		if (key == null)
			return;
		
		if (!bloomFilter.contains(key))
		{
			bloomFilter.add(key);
			count.incrementAndGet();
		}
	}
}
