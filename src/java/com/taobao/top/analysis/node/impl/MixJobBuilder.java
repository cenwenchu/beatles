/**
 * 
 */
package com.taobao.top.analysis.node.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.job.Job;
import com.taobao.top.analysis.node.IJobBuilder;

/**
 * 支持多个扩展的JobBuilder,默认集成了本地文件的builder
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class MixJobBuilder implements IJobBuilder {

	Map<String,IJobBuilder> jobBuilders;
	private MasterConfig config;
	
	@Override
	public MasterConfig getConfig() {
		return config;
	}


	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}
	
	public void addBuilder(String protocol,IJobBuilder jobBuilder)
	{
		jobBuilders.put(protocol, jobBuilder);
	}
	
	public void removeBuilder(String protocol)
	{
		jobBuilders.remove(protocol);
	}
	
	@Override
	public List<Job> build(String config) throws 
			AnalysisException {
		
		String protocol = "file";
		String conf = config;
		IJobBuilder jobBuilder = jobBuilders.get(protocol);
		
		if (config.indexOf(":") > 0)
		{
			protocol = config.substring(0,config.indexOf(":"));
			conf = config.substring(config.indexOf(":")+1);
			
			if (jobBuilders.containsKey(protocol))
				jobBuilder = jobBuilders.get(protocol);		
		}
		
		return jobBuilder.build(conf);
	}

	@Override
	public void init() {
		jobBuilders = new HashMap<String,IJobBuilder>();
		jobBuilders.put("file", new FileJobBuilder());
	}

	@Override
	public void releaseResource() {
		jobBuilders.clear();
	}

}
