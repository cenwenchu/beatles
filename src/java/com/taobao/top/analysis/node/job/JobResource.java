package com.taobao.top.analysis.node.job;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.util.AnalyzerFilenameFilter;


/**
 * JobResource.java
 * 
 * @author yunzhan.jtq
 * 
 * @since 2012-2-20 下午01:23:50
 */
public class JobResource {
    private static final Log logger = LogFactory.getLog(JobResource.class);
    private String job;
    private File resource;
    private Map<String, String> files;

    private long lastLoadTime;


    public JobResource(String job, String[] pathes) {
        this.job = job;
        if(pathes == null || pathes.length == 0)
            throw new java.lang.RuntimeException("JobResource path can't be null...");
        files = new HashMap<String, String>();
        for(String path : pathes) {
            this.build(path);
        }
        lastLoadTime = System.currentTimeMillis();
    }
    
    private void build(String path) {
        if (path.startsWith("file:"))
            resource = new File(path.substring("file:".length()));
        else if(path.startsWith("dir:")) {
            resource = new File(path.substring("dir:".length()));
            if (resource.isDirectory()) {
                File[] fs = resource.listFiles(new AnalyzerFilenameFilter(".xml"));

                for (File f : fs) {
                    files.put(f.getName(), f.getName());
                }
            }
            return;
        }
        else
            resource = new File(path);


        if (resource.exists()) {
            if (!path.startsWith("file:")) {
                URL url = Thread.currentThread().getContextClassLoader().getResource(path);

                if (url == null)
                    throw new java.lang.RuntimeException("It is not a validate jobResource..." + path);
                else
                    resource = new File(url.getFile());
            }
            else
                throw new java.lang.RuntimeException("It is not a validate jobResource..." + path);
        }
        
        files.put(resource.getName(), resource.getName());
    }


    public File getResource() {
        return resource;
    }


    /**
     * 判断是否被修改
     * 
     * @return
     */
    public boolean isModify() {

        if (resource.isDirectory()) {
            File[] fs = resource.listFiles(new AnalyzerFilenameFilter(".xml"));
            if(fs.length != files.size())
                return true;

            for (File f : fs) {
                if ((f.lastModified() > lastLoadTime) || (files.get(f.getName()) == null)) {

                    logger.info("file: " + f.getName() + " is modify," + "lastloadtime:" + lastLoadTime + ",file time:"
                            + f.lastModified());
                    return true;
                }
            }
        }
        else {
            if (resource.lastModified() > lastLoadTime) {
                return true;
            }
        }

        return false;
    }


    /**
     * 重新载入,如果newPath是null，就直接更新原有目录
     * 
     * @param newPath
     */
    @Deprecated
    public void reload(String newPath) {
        if (newPath != null) {
            resource = new File(newPath);

            if (!resource.exists() || (resource.exists() && !resource.isDirectory()))
                throw new java.lang.RuntimeException("It is not a validate dir..." + newPath);
        }

        files.clear();

        if (resource.isDirectory()) {
            File[] fs = resource.listFiles(new AnalyzerFilenameFilter(".xml"));

            for (File f : fs) {
                files.put(f.getName(), f.getName());
            }
        }
        else {
            files.put(resource.getName(), resource.getName());
        }

        lastLoadTime = System.currentTimeMillis();
    }


    public final String getJob() {
        return job;
    }
}
