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

import com.taobao.top.analysis.node.job.JobTask;

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
		
		String input = jobtask.getInput();
		
		if (input.indexOf("file:") >= 0)
			input = input.substring("file:".length());
		
		File file = new File(input);
		URL fileResource = null;

		if (!file.exists()) {
			fileResource = ClassLoader.getSystemResource(input);
			if (fileResource == null)
				throw new java.io.FileNotFoundException(
						"Job resource not exist,file : "
								+ input);
			else
				logger.warn("load resource form classpath :"
						+ fileResource.getFile());
		}
		else
		{
			if (file.isDirectory())
				throw new java.io.FileNotFoundException(
						"Job resource is directory,file : "
								+ input);
		}

		if (fileResource == null)
			return new FileInputStream(file);
		else
			return fileResource.openStream();
	}

	
	@Override
	public boolean ignore(String input) {
		
		if (input.indexOf(":") < 0)
			return false;
		else
			return input.indexOf("file:") < 0;
	}

}
