/**
 * 
 */
package com.taobao.top.analysis.util;

import java.io.File;
import java.io.FilenameFilter;

/**
 * 
 * @author zijiang.jl
 * 
 */
public class ExtFilenameFilter implements FilenameFilter {
	private String extension;

	public ExtFilenameFilter(String ext) {
		extension = ext;
	}

	@Override
	public boolean accept(File dir, String name) {
		File f=new File(dir.getAbsolutePath()+File.separator+name);
		if(f.isDirectory()) return true;
		if(f.isFile()&&name.endsWith(extension)) return true;
		return false;
	}
}
