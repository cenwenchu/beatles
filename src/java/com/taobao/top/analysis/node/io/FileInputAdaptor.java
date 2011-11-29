/**
 * 
 */
package com.taobao.top.analysis.node.io;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.job.JobTask;

/**
 * 文件输入适配器
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class FileInputAdaptor implements IInputAdaptor {

	private static final Log logger = LogFactory.getLog(FileInputAdaptor.class);
	
	@Override
	public InputStream getInputFormJob(JobTask jobtask) throws IOException {
		
		File file = new File(jobtask.getInput().substring("file:".length()));
		URL fileResource = null;

		if (!file.exists()) {
			fileResource = ClassLoader.getSystemResource(jobtask.getInput()
					.substring("file:".length()));
			if (fileResource == null)
				throw new java.io.FileNotFoundException(
						"Job resource not exist,file : "
								+ jobtask.getInput().substring("file:".length()));
			else
				logger.warn("load resource form classpath :"
						+ fileResource.getFile());
		}
		else
		{
			if (file.isDirectory())
				throw new java.io.FileNotFoundException(
						"Job resource is directory,file : "
								+ jobtask.getInput().substring("file:".length()));
		}

		if (fileResource == null)
			return new FileInputStream(file);
		else
			return fileResource.openStream();
	}

	
	@Override
	public boolean ignore(String input) {
		return input.indexOf("file:") < 0;
	}

}
