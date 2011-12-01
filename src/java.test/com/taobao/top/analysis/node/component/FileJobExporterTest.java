/**
 * 
 */
package com.taobao.top.analysis.node.component;

import static org.junit.Assert.*;

import java.util.Map;
import org.junit.Test;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.job.Job;

/**
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午1:13:56
 *
 */
public class FileJobExporterTest {

	
	@Test
	public void testExportReportJob() throws AnalysisException {
		MixJobBuilder mixJobBuilder = new MixJobBuilder();
		FileJobExporter fileJobExporter = new FileJobExporter();
		MasterConfig config = new MasterConfig();
		config.load("master-config.properties");
		fileJobExporter.setConfig(config);
		mixJobBuilder.setConfig(config);
		mixJobBuilder.init();
		fileJobExporter.init();
		
		Map<String, Job> jobs = mixJobBuilder.build();
		
		for(Job job : jobs.values())
			fileJobExporter.exportReport(job, false);
		
		mixJobBuilder.releaseResource();
		fileJobExporter.releaseResource();
	}

	
	@Test
	public void testExportReportJobTaskJobTaskResult() {
		fail("Not yet implemented");
	}

	
	@Test
	public void testExportEntryData() {
		fail("Not yet implemented");
	}

	
	@Test
	public void testLoadEntryData() {
		fail("Not yet implemented");
	}

	
	@Test
	public void testLoadEntryDataToTmp() {
		fail("Not yet implemented");
	}

}
