/**
 * 
 */
package com.taobao.top.analysis.node.base.impl;


import com.taobao.top.analysis.job.JobTask;
import com.taobao.top.analysis.node.base.IOutputAdaptor;
import com.taobao.top.analysis.node.base.IReportExporter;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class FileOutputAdaptor implements IOutputAdaptor {
	
	//private static final Log logger = LogFactory.getLog(FileOutputAdaptor.class);
	
	IReportExporter reportExporter; 
	
	public IReportExporter getReportExporter() {
		return reportExporter;
	}


	public void setReportExporter(IReportExporter reportExporter) {
		this.reportExporter = reportExporter;
	}


	@Override
	public void sendResultToOutput(JobTask jobtask) {
		reportExporter.generateReports(jobtask, false);
	}

	
	@Override
	public boolean ignore(String output) {
		return output.indexOf("file:") < 0;
	}

}
