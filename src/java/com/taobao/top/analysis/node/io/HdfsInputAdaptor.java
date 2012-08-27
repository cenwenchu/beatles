/**
 * 
 */
package com.taobao.top.analysis.node.io;

import java.io.InputStream;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午1:19:27
 *
 */
public class HdfsInputAdaptor implements IInputAdaptor {
    private static final Log logger = LogFactory.getLog(HdfsInputAdaptor.class);

	static
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	
	@Override
    public InputStream getInputFormJob(JobTask jobtask, JobTaskExecuteInfo taskExecuteInfo) {
        try {
            URL url = new URL(jobtask.getInput());
            return url.openStream();
        }
        catch (Throwable e) {
            logger.error("job get input error:" + jobtask.getJobName() + "," + jobtask.getInput(), e);
        }
        return null;
    }

	
	@Override
	public boolean ignore(String input) {
		return input.indexOf("hdfs:") < 0;
	}

}
