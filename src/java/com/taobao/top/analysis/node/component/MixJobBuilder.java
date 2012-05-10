/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobBuilder;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.util.AnalyzerUtil;

/**
 * 支持多个扩展的JobBuilder,默认集成了本地文件的builder
 * 此处有陷阱，支持多种类型的jobBuilder切换时注意
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-28
 *
 */
public class MixJobBuilder implements IJobBuilder {
    private static final Log log = LogFactory.getLog(MixJobBuilder.class);

	Map<String,IJobBuilder> jobBuilders;
	private MasterConfig config;
	private IJobBuilder jobBuilder;
	private String jobResource;
	
	@Override
	public boolean isNeedRebuild() {
		if (jobBuilder == null)
			return false;
		else
			return jobBuilder.isNeedRebuild();
	}

	@Override
	public void setNeedRebuild(boolean needRebuild) {
		if (jobBuilder != null)
			jobBuilder.setNeedRebuild(true);
	}


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
	
	public Map<String,Job> build() throws AnalysisException 
	{
		if(config == null)
			throw new AnalysisException("master config is null!");
			
		return build(config.getJobsSource());
	}
	
	@Override
	public Map<String,Job> build(String config) throws 
			AnalysisException {
				
		String protocol = "file";
//		String conf = config;
		jobResource = config;
		jobBuilder = jobBuilders.get(protocol);
		
		if (config.indexOf(":") > 0)
		{
			protocol = config.substring(0,config.indexOf(":"));
//			conf = config.substring(config.indexOf(":")+1);
			
			if (jobBuilders.containsKey(protocol))
				jobBuilder = jobBuilders.get(protocol);		
		}
		
		return jobBuilder.build(config);
	}

	@Override
	public void init() {
		jobBuilders = new HashMap<String,IJobBuilder>();
		FileJobBuilder fileJobBuilder = new FileJobBuilder();
		fileJobBuilder.init();
		jobBuilders.put("file", fileJobBuilder);
		jobBuilders.get("file").setConfig(config);
		if(log.isInfoEnabled()) {
		    log.info("mixJobBuilder init complete.");
		}
	}

	@Override
	public void releaseResource() {
		jobBuilders.clear();
	}

	@Override
	public Map<String,Job> rebuild(Map<String,Job> jobs) throws AnalysisException {
	    //此处有陷阱，支持多种类型的jobBuilder切换时注意
		if (this.isNeedRebuild())
		{
		    return jobBuilder.rebuild(jobs);
		}
		else
			return null;
	}

	@Override
	public void buildTasks(Job job) throws AnalysisException {
		// TODO Auto-generated method stub
		
	}

    /* (non-Javadoc)
     * @see com.taobao.top.analysis.node.IJobBuilder#isModified()
     */
    @Override
    public boolean isModified() {
        //此处有陷阱，支持多种类型的jobBuilder切换时注意
        if(!AnalyzerUtil.covertNullToEmpty(config.getJobsSource()).equals(AnalyzerUtil.covertNullToEmpty(this.jobResource)))
            return true;
        if(this.jobBuilders.get("file").isModified())
            return true;
        return false;
    }

    /* (non-Javadoc)
     * @see com.taobao.top.analysis.node.IJobBuilder#getJobResource()
     */
    @Override
    public String getJobResource() {
        return jobResource;
    }

}
