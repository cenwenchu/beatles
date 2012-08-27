/**
 * 
 */
package com.taobao.top.analysis.node.io;


import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;

/**
 * Http数据源适配器
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class HttpInputAdaptor implements IInputAdaptor {
    private static final Log logger = LogFactory.getLog(HttpInputAdaptor.class);

	@Override
    public InputStream getInputFormJob(JobTask jobtask, JobTaskExecuteInfo taskExecuteInfo) {
        try {
            //特殊处理，临时，由于tbSession的bug
            //不能直接对ip访问，需要加上header host
            boolean check = false;
            String input = jobtask.getInput();
            if(jobtask.getInput().startsWith("shttp://")) {
                check = true;
                input = input.replaceAll("shttp://", "http://");
            }
            URL url = new URL(input);
            URLConnection conn = url.openConnection();
            if(check)
                conn.addRequestProperty("host", "qinglie.taobao.com");
            conn.setConnectTimeout(30000);
            return conn.getInputStream();
        }
        catch (Throwable e) {
            logger.error("job get input error:" + jobtask.getJobName() + "," + jobtask.getInput(), e);
        }
        return null;
    }

	@Override
	public boolean ignore(String input) {
		return input.indexOf("http:") < 0;
	}

}
