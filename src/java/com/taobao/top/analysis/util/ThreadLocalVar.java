package com.taobao.top.analysis.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * ThreadLocalVar.java
 * @author yunzhan.jtq
 * 
 * @since 2012-8-16 下午12:01:09
 */
public class ThreadLocalVar {
    /**
     * 目前默认支持一种long型时间戳
     * 一种2012-07-17 00:14:08格式的时间戳
     * 暂不支持配置
     */
    private static final String sdf = "yyyy-MM-dd HH:mm:ss";
    
    private static ThreadLocal<DateFormat> threadLocalDate = new ThreadLocal<DateFormat>();
    
    public static DateFormat getDateFormat() {
        DateFormat df = threadLocalDate.get();
        if(df == null) {
            df = new SimpleDateFormat(sdf);
            threadLocalDate.set(df);
        }
        return df;
    }
}
