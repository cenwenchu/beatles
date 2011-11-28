/**
 * 
 */
package com.taobao.top.analysis.node.impl;

import java.util.List;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.job.Job;
import com.taobao.top.analysis.node.IJobBuilder;
import com.taobao.top.analysis.node.IJobExporter;
import com.taobao.top.analysis.node.IJobManager;
import com.taobao.top.analysis.node.IJobResultMerger;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class JobManager implements IJobManager {

	private IJobBuilder jobBuilder;
	private IJobExporter jobExporter;
	private IJobResultMerger jobResultMerger;
	private boolean needReloadJobs;
	private MasterConfig config;
	
	@Override
	public MasterConfig getConfig() {
		return config;
	}


	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}

	@Override
	public void init() throws AnalysisException {
		jobBuilder.init();
		jobExporter.init();
		jobResultMerger.init();
	}

	
	@Override
	public void releaseResource() {
		jobBuilder.releaseResource();
		jobExporter.releaseResource();
		jobResultMerger.releaseResource();
	}
	
	@Override
	public void checkJobStatus(List<Job> jobs) {
		// TODO Auto-generated method stub

	}

	
	@Override
	public IJobBuilder getJobBuilder() {
		return jobBuilder;
	}

	
	@Override
	public void setJobBuilder(IJobBuilder jobBuilder) {
		this.jobBuilder = jobBuilder;
	}

	
	@Override
	public IJobExporter getJobExporter() {
		return jobExporter;
	}

	
	@Override
	public void setJobExporter(IJobExporter jobExporter) {
		this.jobExporter = jobExporter;
	}

	
	@Override
	public IJobResultMerger getJobResultMerger() {
		return jobResultMerger;
	}

	
	@Override
	public void setJobResultMerger(IJobResultMerger jobResultMerger) {
		this.jobResultMerger = jobResultMerger;
	}

	@Override
	public boolean isNeedReloadJobs() {
		return needReloadJobs;
	}

	@Override
	public void setNeedReloadJobs(boolean needReloadJobs) {
		this.needReloadJobs = needReloadJobs;
	}

}
