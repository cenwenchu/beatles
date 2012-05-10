/**
 * 
 */
package com.taobao.top.analysis.node.component;


import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.job.Job;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 上午11:48:01
 *
 */
public class JobBuilderTest {


	@Test
	public void testBuild() throws AnalysisException {
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		FileJobBuilder fileJobBuilder = new FileJobBuilder();
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		mixJobBuilder.setConfig(config);
		mixJobBuilder.init();
		fileJobBuilder.setConfig(config);
		fileJobBuilder.init();
		
		Map<String, Job> jobs = fileJobBuilder.build();
		
		Assert.assertEquals(2, jobs.size());
		
		Assert.assertTrue(jobs.containsKey("job1"));
		Assert.assertTrue(jobs.containsKey("job2"));
		
		jobs = mixJobBuilder.build();
		
		Assert.assertEquals(2, jobs.size());
		
		Assert.assertTrue(jobs.containsKey("job1"));
		Assert.assertTrue(jobs.containsKey("job2"));
		
		fileJobBuilder.releaseResource();
		mixJobBuilder.releaseResource();
	}

	
	@Test
	public void testRebuild() throws AnalysisException {
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		mixJobBuilder.setConfig(config);
		mixJobBuilder.init();
		
		Map<String, Job> jobs = mixJobBuilder.build();
		
		Assert.assertEquals(2, jobs.size());
		
		Assert.assertTrue(jobs.containsKey("job1"));
		Assert.assertTrue(jobs.containsKey("job2"));
		jobs.clear();
		
		jobs = mixJobBuilder.rebuild(jobs);
		
		Assert.assertNull(jobs);
		
		mixJobBuilder.setNeedRebuild(true);
		jobs = mixJobBuilder.rebuild(jobs);
		
		Assert.assertEquals(2, jobs.size());
		
		Assert.assertTrue(jobs.containsKey("job1"));
		Assert.assertTrue(jobs.containsKey("job2"));
		
		mixJobBuilder.releaseResource();
	}

}
