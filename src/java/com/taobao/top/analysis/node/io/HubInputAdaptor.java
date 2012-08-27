package com.taobao.top.analysis.node.io;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;

/**
 * HubInputAdaptor.java
 * @author yunzhan.jtq
 * 
 * @since 2012-5-16 下午02:11:23
 */
public class HubInputAdaptor implements IInputAdaptor {
    private static final Log logger = LogFactory.getLog(HubInputAdaptor.class);
    private SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd");

    /* (non-Javadoc)
     * @see com.taobao.top.analysis.node.io.IInputAdaptor#getInputFormJob(com.taobao.top.analysis.node.job.JobTask)
     * hub获取数据会有一段逻辑处理，目的在于游标管理、跨天数据处理
     * 目前这种方式的hub拉取，有一个不可避免掉的问题————单行错误数据，该问题通过目前的方式不太容易解决
     */
    @Override
    public InputStream getInputFormJob(JobTask jobtask, JobTaskExecuteInfo taskExecuteInfo) {
        try {
            String input = jobtask.getInput();
            String uri = input.replaceAll("hub:", "http:");
            Long begin = Long.parseLong(jobtask.getInput().substring(jobtask.getInput().indexOf("&begin=") + 7, jobtask.getInput().indexOf("&end=")));
            Long end = Long.parseLong(jobtask.getInput().substring(jobtask.getInput().indexOf("&end=") + 5));
            URL url;
            URLConnection conn;
            
            //首先获取今日日志的大小
            String temp = uri.substring(0, uri.indexOf('?'));
            temp = temp.replaceAll("/get/", "/size/");
            url = new URL(temp);
            conn = url.openConnection();
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String record = reader.readLine();
            if(!StringUtils.isNumeric(record)) {
                logger.error(input + " size is " + record);
                return null;
            }
            Long size = Long.parseLong(record);
            //重置游标处理
            if (jobtask.getTailCursor().get()) {
                taskExecuteInfo.setFileBegin(0);
                taskExecuteInfo.setFileLength(size);
                taskExecuteInfo.setTimestamp(System.currentTimeMillis());
                return conn.getInputStream();
            }
            
            //如果不是重置游标，则先检查游标是否超过日志大小
            //超过日志大小，对比前一日日志的大小以及当前时间，如果是已经跨天，则直接读新的日志
            //否则不读任何数据
            //目前此处跨天的处理逻辑
            if (size < (begin+2)) {
                logger.error(input + " size is " + record + " and begin is " + begin);
                if(!temp.endsWith(".log")) {
                    taskExecuteInfo.setFileBegin(begin);
                    taskExecuteInfo.setFileLength(0L);
                    taskExecuteInfo.setTimestamp(System.currentTimeMillis());
                    return conn.getInputStream();
                }
                temp = temp + "." + getDate();
                url = new URL(temp);
                conn = url.openConnection();
                conn.setConnectTimeout(30000);
                reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                record = reader.readLine();
                long time = System.currentTimeMillis();
                long lastTime = jobtask.getJobSourceTimeStamp();
                long last = (lastTime + 28800000L) / 86400000;
                long now = (time + 28800000L) / 86400000;
                taskExecuteInfo.setFileBegin(0L);
                taskExecuteInfo.setFileLength(0L);
                taskExecuteInfo.setTimestamp(time);
                logger.error(input + " size is " + record + " and begin is " + begin + ",now is " + now + ", last is " + last);
                if(StringUtils.isNumeric(record)) {
                    Long lastSize = Long.parseLong(record);
                    if(lastSize < begin) {
                        return conn.getInputStream();
                    }
                    if((time+ 28800000L) % 86400000 > 300000) {
                        return conn.getInputStream();
                    }
                    if((lastTime == 0 && (time+ 28800000L) % 86400000 < 300000) || now > last) {
                        temp = temp.replaceAll("/size/", "/get/");
                        if(temp.contains("?")) 
                            temp += "&";
                        else
                            temp += "?";
                        temp = temp + "begin=" + begin + "&end=" + end;
                        url = new URL(temp);
                        conn = url.openConnection();
                        conn.setConnectTimeout(30000);
                        return conn.getInputStream();
                    } else {
                        taskExecuteInfo.setFileBegin(begin);
                        return conn.getInputStream();
                    }
                } else {
                    taskExecuteInfo.setFileBegin(begin);
                    return conn.getInputStream();
                }
            }
            
            
            url = new URL(uri);
            conn = url.openConnection();
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            

            taskExecuteInfo.setFileBegin(Long.parseLong(conn.getHeaderField("file-begin")));
            taskExecuteInfo.setFileLength(Long.parseLong(conn.getHeaderField("file-length")));
            taskExecuteInfo.setTimestamp(System.currentTimeMillis());
            if (!uri.contains("encode=text")) {
                GZIPInputStream gzipin = new GZIPInputStream(conn.getInputStream());
                return gzipin;
            }
            return conn.getInputStream();
        }
        catch (Throwable e) {
            logger.error("job get input error:" + jobtask.getJobName() + "," + jobtask.getInput(), e);
        }
        return null;
    }
    
    private String getDate() {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, -1);
        Date date = c.getTime();
        return simple.format(date);
    }


    /* (non-Javadoc)
     * @see com.taobao.top.analysis.node.io.IInputAdaptor#ignore(java.lang.String)
     */
    @Override
    public boolean ignore(String input) {
        return input.indexOf("hub:") < 0;
    }

}
