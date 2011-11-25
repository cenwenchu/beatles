/**
 * 
 */
package com.taobao.top.analysis.node.base;


import java.io.IOException;
import java.io.InputStream;

import com.taobao.top.analysis.job.JobTask;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public interface IInputAdaptor {
	
	InputStream getInputFormJob(JobTask jobtask) throws IOException;
	
	boolean ignore(String input);

}
