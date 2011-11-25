/**
 * 
 */
package com.taobao.top.analysis.node.base;


import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.taobao.top.analysis.job.JobTask;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public interface IAnalysisEngine {
	
	public void addInputAdaptor(IInputAdaptor inputAdaptor);
	public void removeInputAdaptor(IInputAdaptor inputAdaptor);
	
	public void addOutputAdaptor(IOutputAdaptor outputAdaptor);
	public void removeOutputAdaptor(IOutputAdaptor outputAdaptor);
	
	public void doAnalysis(JobTask jobTask) throws UnsupportedEncodingException,IOException;

}
