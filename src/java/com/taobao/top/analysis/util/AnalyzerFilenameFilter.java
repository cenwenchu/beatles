/**
 * 
 */
package com.taobao.top.analysis.util;

import java.io.File;
import java.io.FilenameFilter;

/**
 * 需要分析的日志文件过滤类，根据后缀名
 * 
 * @author fangweng
 * 
 */
public class AnalyzerFilenameFilter implements FilenameFilter {
	String extension;
	String prefix = "";

	public AnalyzerFilenameFilter(String ext) {
		extension = ext;
	}
	
	public AnalyzerFilenameFilter(String ext, String prefix) {
	    this.extension = ext;
	    this.prefix = prefix;
	}

	@Override
	public boolean accept(File dir, String name) {

		if (name.startsWith(prefix) && name.endsWith(extension))
			return true;
		else
			return false;
	}
}
