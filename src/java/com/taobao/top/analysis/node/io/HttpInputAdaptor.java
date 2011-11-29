/**
 * 
 */
package com.taobao.top.analysis.node.io;


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import com.taobao.top.analysis.job.JobTask;

/**
 * Http数据源适配器
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class HttpInputAdaptor implements IInputAdaptor {

	@Override
	public InputStream getInputFormJob(JobTask jobtask) throws IOException {
		URL url = new URL(jobtask.getInput());
		return url.openConnection().getInputStream();
	}

	@Override
	public boolean ignore(String input) {
		return input.indexOf("http:") < 0;
	}

}
