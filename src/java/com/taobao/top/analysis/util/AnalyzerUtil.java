package com.taobao.top.analysis.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.node.job.Job;

/**
 * Analyzer中的一些公共方法
 * AnalyzerUtil.java
 * @author yunzhan.jtq
 * 
 * @since 2012-2-8 下午01:30:45
 */
public class AnalyzerUtil {
    private static final Log logger = LogFactory.getLog(AnalyzerUtil.class);
    private static long buildDate=-1;
    private static final String report2MapFile = "report2Map";
    private static ThreadPoolExecutor alertThreadPool = new ThreadPoolExecutor(
        2, 5, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("Sendder_worker"));
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 获取MANIFEST.MF文件中的属性
     * @param attr
     * @return
     */
    public static String getManifestAttr(String attr){
        try {
            InputStream is=Thread.currentThread().getContextClassLoader().getResourceAsStream("META-INF/MANIFEST.MF");
            Manifest mani=new Manifest();
            mani.read(is);
            Attributes atr=mani.getMainAttributes();
            String val=atr.getValue(attr);
            return val;
        } catch (Throwable e) {
            logger.error(e);
        }
        return null;
    }
    
    /**
     * 获取MANIFEST.MF文件的创建时间作为打包时间
     * @return
     */
    public static long getManifestBuildDate(){
        if(buildDate!=-1) return buildDate;
        String str=getManifestAttr("BuildDate");
        try{
            return Long.parseLong(str);
        }catch(Throwable t){
            return buildDate;
        }
    }
    
    public static void sendOutAlert(final Calendar calendar,
            final String alertURL, final String alertFrom,
            final String alertModel, final String alertWangwang,
            final String content) {

        alertThreadPool.execute(new Runnable() {

            @Override
            public void run() {
                StringBuilder urlStr = new StringBuilder(alertURL);
                urlStr.append("?");
                urlStr.append("from=");
                urlStr.append(alertFrom);
                urlStr.append("&");
                urlStr.append("alertModel=");
                urlStr.append(alertModel);
                urlStr.append("&");
                urlStr.append("wangwang=");
                String temp = "error!";
                String temp1 = "error!";
                try {
                    String date = sdf.format(java.util.Calendar.getInstance().getTime());
                    temp = java.net.URLEncoder.encode(alertWangwang, "UTF-8");
                    temp1 = java.net.URLEncoder.encode(content + "(" + date
                            + "," + ReportUtil.getIp() + ")", "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    logger.error(e.getMessage(), e);
                }
                urlStr.append(temp);
                urlStr.append("&");
                urlStr.append("content=");
                urlStr.append(temp1);

                HttpURLConnection con = null;
                InputStream is = null;
                try {
                    java.net.URL url = new java.net.URL(urlStr.toString());
                    con = HttpURLConnection.class.cast(url.openConnection());
                    con.setRequestMethod("GET");
                    con.setRequestProperty("Content-Type",
                            "text/html; charset=UTF-8");
                    con.connect();
                    is = con.getInputStream();
                } catch (Throwable e) {
                    logger.error(urlStr.toString(), e);
                } finally {
                    try {
                        if (is != null)
                            is.close();
                        if (con != null)
                            con.disconnect();
                    } catch (Throwable e) {
                        logger.error(e.getMessage(), e);
                    }

                }

            }

        });
    }
    
    /**
     * 将NULL转为空字符串
     * @param str
     * @return
     */
    public static String covertNullToEmpty(String str) {
        if (str == null)
            return "";
        return str;
    }
    
    /**
     * 临时使用方法
     * 将report2Master进行备份
     * 下次加载规则时，如果存在该文件，则优先加载该文件
     * 加载该文件后，再根据配置进行调整，没有的report和master从该配置中去除
     * @return
     */
    public static boolean exportReportToMaster(Map<String, String> report2Master, final Job job) {

        if (report2Master == null || report2Master.size() <= 0)
            return true;

        BufferedWriter bwriter = null;
        String _fileSuffix = AnalysisConstants.INNER_DATAFILE_SUFFIX;
        String _bckSuffix = AnalysisConstants.IBCK_DATAFILE_SUFFIX;
        String destfile = null;

        try {
            String dir = getDir(job.getJobConfig().getOutput());
            File dest = new File(dir);
            if (!dest.exists() || (dest.exists() && !dest.isDirectory()))
                dest.mkdirs();

            if (!dir.endsWith(File.separator))
                dir += File.separator;

            destfile =
                    new StringBuilder(dir).append(job.getJobName()).append("-").append(report2MapFile).append("-")
                        .append(ReportUtil.getIp()).append(_fileSuffix).toString();

            File bckFile = new File(destfile.replace(_fileSuffix, _bckSuffix));
            if (bckFile.exists())
                bckFile.delete();
            File f = new File(destfile);

            if (f.exists())
                f.renameTo(new File(destfile.replace(_fileSuffix, _bckSuffix)));
            else
                f.createNewFile();
            bwriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f.getAbsolutePath(), false)));

            // 写个开头

            Iterator<String> keys = report2Master.keySet().iterator();

            while (keys.hasNext()) {
                String key = keys.next();

                bwriter.write(key);
                bwriter.write(AnalysisConstants.EXPORT_RECORD_SPLIT);

                String value = report2Master.get(key);
                bwriter.write(value);

                bwriter.write(ReportUtil.getSeparator());

            }
            bwriter.flush();
            if(logger.isInfoEnabled())
                logger.info("export report2Map into file : " + destfile);
            return true;

        }
        catch (Throwable ex) {
            logger.error(ex);
            throw new RuntimeException(ex);
        }
        finally {
            if (bwriter != null) {
                try {
                    bwriter.close();
                }
                catch (IOException e) {
                    logger.error(e);
                }
            }

        }
    }
    
    /**
     * 临时策略
     * 将report2Master文件load进内存，并与按照分配策略所产生的Map进行合并
     * 合并策略为：
     * 1、首先去除所load报表中没有的
     * 2、其次取出所load配置中不存在的master
     * 3、将剩余的覆盖进按照分配策略所产生的map,覆盖时info级别打点
     * @return
     */
    public static Map<String, String> loadReportToMaster(List<String> masters, List<String> reports,
            Map<String, String> report2Master, final Job job) {
        BufferedReader breader = null;

        Map<String, String> resultPool = null;

        String _fileSuffix = AnalysisConstants.INNER_DATAFILE_SUFFIX;
        String destfile = null;
        long beg = System.currentTimeMillis();
        String dir = getDir(job.getJobConfig().getOutput());
        File dest = new File(dir);
        if (!dest.exists() || (dest.exists() && !dest.isDirectory()))
            return report2Master;
        if (!dir.endsWith(File.separator))
            dir += File.separator;

        destfile =
            new StringBuilder(dir).append(job.getJobName()).append("-").append(report2MapFile).append("-")
                .append(ReportUtil.getIp()).append(_fileSuffix).toString();
        File f = new File(destfile);

        if (!f.exists())
            return report2Master;

        try {
            breader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

            String line;
            while ((line = breader.readLine()) != null) {
                if (resultPool == null)
                    resultPool = new HashMap<String, String>();

                String[] contents = StringUtils.splitByWholeSeparator(line, AnalysisConstants.EXPORT_RECORD_SPLIT);

                if (contents == null || contents.length != 2)
                    continue;

                if (masters != null && !masters.contains(contents[1]))
                    continue;
                if (reports != null && !reports.contains(contents[0]))
                    continue;
                resultPool.put(contents[0], contents[1]);

            }

            if (logger.isWarnEnabled())
                logger.warn("Load file " + f.getAbsolutePath() + " Success , use : "
                        + String.valueOf(System.currentTimeMillis() - beg));

            if(resultPool == null)
                return report2Master;
            for (String key : resultPool.keySet()) {
                report2Master.put(key, resultPool.get(key));
            }
        }
        catch (Exception ex) {
            logger.error("Load file " + f.getAbsolutePath() + " Error", ex);
            throw new RuntimeException("Load file " + f.getAbsolutePath() + " Error", ex);
        }
        finally {
            if (breader != null) {
                try {
                    breader.close();
                }
                catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return report2Master;
    }
    
    /**
     * 将string转为map简单实现
     * @param string
     * @param recordSplit
     * @param mapSplit
     * @return
     */
    public static Map<String, String> convertStringToMap(String string, String recordSplit, String mapSplit) {
        if(string == null)
            return new HashMap<String, String>();
        String[] records = StringUtils.splitByWholeSeparator(string, recordSplit);
        if(records == null || records.length <= 0)
            return new HashMap<String, String>();
        Map<String, String> result = new HashMap<String, String>();
        for(String record : records) {
            String ss[] = StringUtils.splitByWholeSeparator(record, mapSplit);
            if(ss == null || ss.length != 2)
                continue;
            result.put(ss[0], ss[1]);
        }
        return result;
    }
    
    private static String getDir(String dir) {
        String rootDir = dir;
        
        //去掉前缀，主要用于协议的前缀
        if (rootDir.indexOf(":") > 0)
            rootDir = rootDir.substring(rootDir.indexOf(":") +1);
        
        if (!rootDir.endsWith(File.separator))
            rootDir = new StringBuilder(rootDir).append(File.separator).toString();
        return rootDir;
    }
}
