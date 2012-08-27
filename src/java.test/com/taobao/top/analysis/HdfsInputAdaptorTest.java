/**
 * 
 */
package com.taobao.top.analysis;


import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.IOUtils;

import com.taobao.top.analysis.config.JobConfig;
import com.taobao.top.analysis.node.io.HdfsInputAdaptor;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午1:22:35
 *
 */
public class HdfsInputAdaptorTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		
		HdfsInputAdaptor hdfsInputAdaptor = new HdfsInputAdaptor();
		JobConfig jobConfig = new JobConfig();
		JobTask jobtask = new JobTask(jobConfig);
		jobtask.setInput("hdfs://localhost:9000/user/apple/top/top-access.log");
		
		java.io.BufferedReader reader = null;
		InputStream in = null;
		
		try
		{
			in = hdfsInputAdaptor.getInputFormJob(jobtask, new JobTaskExecuteInfo());
			
			reader = new java.io.BufferedReader(new java.io.InputStreamReader(in));
			
			String aa = null;
			
			while((aa = reader.readLine()) != null)
			{
				System.out.println(aa);
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		finally
		{
			if (in != null)
				IOUtils.closeStream(in);
			
			if (reader != null)
			{
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		

	}

}
