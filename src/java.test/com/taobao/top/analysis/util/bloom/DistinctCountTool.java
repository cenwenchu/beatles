/**
 * 
 */
package com.taobao.top.analysis.util.bloom;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.statistics.data.DistinctCountEntryValue;


/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 上午12:19:43
 *
 */
public class DistinctCountTool {
	
	public static void main(String[] args) throws IOException
	{
		if (args == null || (args != null && args.length > 3))
			System.out.println("DistinctTool command : destFiles groupby distinctcolumn bloomFileMaxKeys errorRate");
		
		long beg = System.currentTimeMillis();
		
		Map<String,DistinctCountEntryValue> result = new HashMap<String,DistinctCountEntryValue>();
		
		String destFiles = args[0];	
		int distinctColumn = Integer.parseInt(args[2]);
		String[] gby = args[1].split(",");
		Integer[] groupby = new Integer[gby.length];
		
		for(int i = 0; i < gby.length; i++)
		{
			groupby[i] = Integer.parseInt(gby[i]);
		}
		
		
		File f = new File(destFiles);
		
		if (f.exists())
		{
			File[] fs;
			if (f.isDirectory())
			{
				fs = f.listFiles();
			}
			else
				fs = new File[]{f};
			
			for(int i = 0 ; i < fs.length; i++)
			{
				if (args.length == 5)
					doDistinct(fs[i],Integer.parseInt(args[3]),Float.parseFloat(args[4]),distinctColumn,groupby,result);
				else
					doDistinct(fs[i],100000,0.0001F,distinctColumn,groupby,result);
			}
			
			
			System.out.println("time consume : " + (System.currentTimeMillis() - beg) + "result size :" + result.size());
			
			new File("out.txt").createNewFile();
			File out = new File("out.txt");
			java.io.BufferedWriter bw = new java.io.BufferedWriter(new java.io.FileWriter(out));
			
			try
			{
				for(Map.Entry<String,DistinctCountEntryValue> e : result.entrySet())
				{
					bw.write(new StringBuilder().append(e.getKey()).append(",").append(e.getValue().getCount()).append("\r\n").toString());
				}
			}
			finally
			{
				if (bw != null)
					bw.close();
			}
			
		}
		else
		{
			System.out.println("desfFiles not exist : " + destFiles);
		}
		
	}
	
	static void doDistinct(File f,int maxKeys,float errorRate,int distinctColumn,Integer[] groupby,Map<String,DistinctCountEntryValue>result) throws IOException
	{
		java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(f));
		
		try
		{
			String c = null;
			
			while((c = br.readLine() )!= null)
			{
				try
				{
					String[] contents = StringUtils.splitByWholeSeparator(c, "%!");
					
					if (contents.length == 0 || (contents.length < distinctColumn))
						continue;
					
					StringBuilder key = new StringBuilder();
					
					for (Integer k : groupby)
					{
						key.append(contents[k]).append("--");
					}
					
					DistinctCountEntryValue distinctEntry = result.get(key.toString());
					
					if (distinctEntry == null)
					{
						distinctEntry = new DistinctCountEntryValue();
						ByteBloomFilter bloomFilter;
						
						bloomFilter = new ByteBloomFilter(maxKeys,errorRate,1);
						distinctEntry.setBloomFilter(bloomFilter);
						result.put(key.toString(), distinctEntry);
					}
					
					distinctEntry.add(contents[distinctColumn]);
				}
				catch(Exception ex)
				{
					System.out.print(ex.getCause());
				}

			}
		}
		finally
		{
			if (br != null)
				br.close();
		}

	}

}
