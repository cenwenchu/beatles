/**
 * 
 */
package com.taobao.top.analysis.util;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.operation.JobDataOperation;
import com.taobao.top.analysis.node.operation.MergeJobOperation;

/**
 * 用于载入master出错时错过的片段数据
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2012-1-17 上午12:45:04
 *
 */
public class MasterDataRecoverWorker extends Thread {
	
	private static final Log logger = LogFactory.getLog(MasterDataRecoverWorker.class);
	
	String tempStoreDataDir;
	Map<String,Job> jobs;
	String masterName;
	boolean isRunnable = true;
	
	public MasterDataRecoverWorker(String masterName,String tempStoreDataDir,Map<String,Job> jobs)
	{
		super("MasterDataRecoverWorker");
		this.tempStoreDataDir = tempStoreDataDir;
		this.jobs = jobs;
		this.masterName = masterName;
	}

	@Override
	public void run() {

		while(isRunnable)
		{
			int count = 0;
			
			try
			{
				File destDir = new File(tempStoreDataDir);
				
				if (!destDir.exists() || (destDir.exists() && !destDir.isDirectory()))
				{
					Thread.sleep(60 * 1000);
					continue;
				}
				
				File[] files = destDir.listFiles(new AnalyzerFilenameFilter(AnalysisConstants.TEMP_MASTER_DATAFILE_SUFFIX));
				
				for(File f : files)
				{
					String fileName = f.getName();
					
					if (!fileName.startsWith(masterName + ":"))
						continue;
					
					String jobName = fileName.substring(fileName.indexOf(AnalysisConstants.SPLIT_KEY) + AnalysisConstants.SPLIT_KEY.length(),
							fileName.indexOf(AnalysisConstants.TEMP_MASTER_DATAFILE_SUFFIX));
					
					Job job = jobs.get(jobName);
					
					List<Map<String, Map<String, Object>>> mergeResults = JobDataOperation.load(f, false);
					
					//如果合并成功，删除临时文件
					if (MergeJobOperation.mergeToTrunk(job, mergeResults))
					{
						f.delete();
					}
					
					count += 1;
					
					logger.info(new StringBuilder("load recover data : ").append(fileName).toString());
				}
				
			}
			catch(InterruptedException e)
			{
				//do nothing
			}
			catch(Exception ex)
			{
				logger.error("MasterDataRecoverWorker error.",ex);
			}
			
			if (count == 0)
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					//do nothing
				}
			
		}
	}
	
	public void stopWorker()
	{
		isRunnable = false;
		this.interrupt();
	}

}
