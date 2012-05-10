/**
 * 
 */
package com.taobao.top.analysis.node.io;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import com.taobao.top.analysis.node.job.JobTask;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午1:19:27
 *
 */
public class HdfsInputAdaptor implements IInputAdaptor {

	static
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	
	@Override
	public InputStream getInputFormJob(JobTask jobtask) throws IOException {
		URL url = new URL(jobtask.getInput());
		return url.openStream();
	}

	
	@Override
	public boolean ignore(String input) {
		return input.indexOf("hdfs:") < 0;
	}

}
