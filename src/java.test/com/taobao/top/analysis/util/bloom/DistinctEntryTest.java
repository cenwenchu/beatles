/**
 * 
 */
package com.taobao.top.analysis.util.bloom;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.taobao.top.analysis.statistics.data.DistinctCountEntryValue;


/**
 * @author fangweng
 * @email fangweng@taobao.com
 * @date 2011-3-16
 *
 */
public class DistinctEntryTest {

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

		DistinctCountEntryValue distinctEntry = new DistinctCountEntryValue();
		ByteBloomFilter bloomFilter = new ByteBloomFilter(10000000,0.0001F,1);
		distinctEntry.setBloomFilter(bloomFilter);
		
		String[][] content = new String[1000][100];
		byte[][][] content1 = new byte[1000][100][];
		
		for(int i =0 ; i < 1000; i++)
			for(int j =0 ; j < 100; j++)
			{
				if ( i > 900 || j > 90)
				{
					content[i][j] = "hello00";
					content1[i][j] = "hello00".getBytes("UTF-8");
				}
				else
				{
					content[i][j] = new StringBuilder("hello").append(i).append(j).toString();
					content1[i][j] = new StringBuilder("hello").append(i).append(j).toString().getBytes("UTF-8");
				}
					
			}
		
		Map <String,String> map = new HashMap<String,String>();
		
		
		
		long beg = System.currentTimeMillis();
		
		for(int i =0 ; i < 1000 ; i++)
		{
			for(int j = 0 ; j < 100; j++)
			{
				//map.put(content[i][j], content[i][j]);
				distinctEntry.add(content1[i][j]);
			}
		}
		
//		System.out.println("count: " + String.valueOf(map.size()));
		System.out.println("count: " + String.valueOf(distinctEntry.getCount()));
		System.out.println("bytesize : " + String.valueOf(distinctEntry.getBloomFilter().getByteSize()));
		System.out.println("已使用内存:" + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024*1024)+"M/");
		System.out.println("消耗时间：" + String.valueOf(System.currentTimeMillis() - beg));
		
		Thread.sleep(2000);
		map.clear();
		Runtime.getRuntime().gc();
		System.out.println("已使用内存:" + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024*1024)+"M/");

	}

}
