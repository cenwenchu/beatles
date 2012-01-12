/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.node.IJobExporter;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.node.operation.CreateReportOperation;
import com.taobao.top.analysis.node.operation.JobDataOperation;
import com.taobao.top.analysis.statistics.data.Report;
import com.taobao.top.analysis.statistics.data.Rule;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.NamedThreadFactory;
import com.taobao.top.analysis.util.ReportUtil;


/**
 * 默认报表输出实现
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class FileJobExporter implements IJobExporter {

	private final Log logger = LogFactory.getLog(FileJobExporter.class);
	/**
	 * 用于输出报表文件的线程池
	 */
	private ThreadPoolExecutor createReportFileThreadPool;
	private MasterConfig config;
	private int maxCreateReportWorker = 8;
	private long lastRuntime=(System.currentTimeMillis() + 8 * 60 * 60 * 1000) / 86400000;
	
	
	public int getMaxCreateReportWorker() {
		return maxCreateReportWorker;
	}


	public void setMaxCreateReportWorker(int maxCreateReportWorker) {
		this.maxCreateReportWorker = maxCreateReportWorker;
	}

	@Override
	public MasterConfig getConfig() {
		return config;
	}


	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}

	@Override
	public void init() {
		
		if (this.config != null)
			maxCreateReportWorker = this.config.getMaxCreateReportWorker();
		
		if(logger.isInfoEnabled())
			logger.info("filejobExporter init end, maxCreateReportWorker size : " + maxCreateReportWorker);
		
		createReportFileThreadPool = new ThreadPoolExecutor(
				maxCreateReportWorker,
				maxCreateReportWorker, 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("createReportFile_worker"));
	}

	
	@Override
	public void releaseResource() {
		createReportFileThreadPool.shutdown();
	}

	@Override
	public List<String> exportReport(Job job,boolean needTimeSuffix) {
		return exportReport(job.getStatisticsRule(),job.getJobConfig().getOutput(),
				job.getJobName(),needTimeSuffix,job.getJobResult(),job.getJobConfig().getOutputEncoding());
	}
	
	@Override
	public List<String> exportReport(JobTask jobTask,JobTaskResult jobTaskResult,boolean needTimeSuffix) {
		
		return exportReport(jobTask.getStatisticsRule(),jobTask.getOutput()
				,jobTask.getTaskId(),needTimeSuffix,jobTaskResult.getResults(),jobTask.getOutputEncoding());
	}
	
	protected List<String> exportReport(Rule statisticsRule,String reportOutput,String id,boolean needTimeSuffix
			,Map<String, Map<String, Object>> entryResultPool,String outputEncoding)
	{
		if (logger.isInfoEnabled())
			logger.info("start exportReport now, id : " + id + ", output : " + reportOutput);
		
		long start = System.currentTimeMillis();
		
		List<String> reports = new CopyOnWriteArrayList<String>();
		
		if (entryResultPool == null || statisticsRule.getReportPool() == null
				|| (entryResultPool != null && entryResultPool.size() == 0)
				|| (statisticsRule.getReportPool() != null
					&& statisticsRule.getReportPool().size() == 0))
			return reports;
		
		//清理lazy数据
		ReportUtil.cleanLazyData(entryResultPool, statisticsRule.getEntryPool());
		//做一下lazy处理，用于输出
		ReportUtil.lazyMerge(entryResultPool, statisticsRule.getEntryPool());

		Calendar calendar = Calendar.getInstance();
		String currentTime = new StringBuilder()
				.append(calendar.get(Calendar.YEAR)).append("-")
				.append(calendar.get(Calendar.MONTH) + 1).append("-")
				.append(calendar.get(Calendar.DAY_OF_MONTH)).toString();

		calendar.add(Calendar.DAY_OF_MONTH, -1);
		String statTime = new StringBuilder()
				.append(calendar.get(Calendar.YEAR)).append("-")
				.append(calendar.get(Calendar.MONTH) + 1).append("-")
				.append(calendar.get(Calendar.DAY_OF_MONTH)).toString();

		String rootDir = reportOutput;
		
		//去掉前缀，主要用于协议的前缀
		if (rootDir.indexOf(":") > 0)
			rootDir = rootDir.substring(rootDir.indexOf(":") +1);
		
		if (!rootDir.endsWith(File.separator))
			rootDir = new StringBuilder(rootDir).append(File.separator).toString();
		
		if (config != null && StringUtils.isNotEmpty(config.getMasterName()))
			rootDir = new StringBuilder(rootDir).append(config.getMasterName())
				.append(File.separator).append(id).append(File.separator).toString();
		else
			rootDir = new StringBuilder(rootDir).append(id).append(File.separator).toString();
		
		StringBuilder periodRootDir = new StringBuilder();
		StringBuilder periodDir = new StringBuilder();
		StringBuilder normalDir = new StringBuilder();
		
		if (rootDir != null) {
			periodRootDir.append(rootDir).append("period").append(File.separator);
			periodDir.append(periodRootDir).append(currentTime).append(File.separator);
			normalDir = new StringBuilder(rootDir).append(currentTime).append(File.separator);
			
			
			File targetDir = new java.io.File(normalDir.toString());
			File period = new java.io.File(periodDir.toString());

			if (!period.exists() || (period.exists() && !period.isDirectory())) {
				period.mkdirs();
			}

			if (!targetDir.exists()
					|| (targetDir.exists() && !targetDir.isDirectory())) {
				targetDir.mkdirs();
			} else {
				// 删除已有的所有的历史文件
				if (targetDir.exists() && targetDir.isDirectory()) {
					File[] deleteFiles = targetDir.listFiles();

					for (File f : deleteFiles)
						f.delete();
				}
			}
		}

		Iterator<Report> iter = statisticsRule.getReportPool()
				.values().iterator();
		
		CountDownLatch countDownLatch = new CountDownLatch(statisticsRule.getReportPool().size());
		List<String> reportFiles = new ArrayList<String>();
		
		while (iter.hasNext()) {
			Report report = iter.next();
			
			//判断是否是自己要输出的报表，在多个master情况下
			if (statisticsRule.getReport2Master() != null 
					&& statisticsRule.getReport2Master().size() > 0 && config != null)
			{
				String r = statisticsRule.getReport2Master().get(report.getId());
				if (!r.startsWith(config.getMasterName()) || !r.endsWith(String.valueOf(config.getMasterPort())))
				{
					countDownLatch.countDown();
					continue;
				}
			}
			
			String reportFile;
			String reportDir = normalDir.toString();
			if(rootDir==null) reportDir = new StringBuilder(report.getFile()).append(File.separator).toString();

			// 增加对于周期性输出的报表处理，就是根据FileName创建目录，目录中文件是FileName+时间戳.
			if (report.isPeriod()) {
				if(start-report.getLastExportTime()>report.getExportInterval()){
					report.setLastExportTime(start);
				}else{
					countDownLatch.countDown();
					continue;
				}
				
				if(report.isAppend()){
					reportDir = new StringBuilder().append(periodRootDir)
					.append(report.getFile()).append(File.separator)
					.toString();
					
				}else{
					reportDir = new StringBuilder().append(periodDir)
					.append(report.getFile()).append(File.separator)
					.toString();
		
				}
				
				File tmpDir = new java.io.File(reportDir);

				if (!tmpDir.exists()
						|| (tmpDir.exists() && !tmpDir.isDirectory())) {
					tmpDir.mkdirs();
				}

			}
			
			if (needTimeSuffix)
				reportFile = new StringBuilder().append(reportDir)
						.append(report.getFile()).append("_")
						.append(statTime).append(".csv").toString();
			else
				reportFile = new StringBuilder().append(reportDir)
						.append(report.getFile()).append(".csv").toString();

			
			// 对周期性输出增加时间戳到文件结尾
			if (report.isPeriod()) {
				if(report.isAppend()){
					long beg=System.currentTimeMillis();
					long currentRuntime = (beg + 8 * 60 * 60 * 1000) / 86400000;
					if(currentRuntime!=lastRuntime){
						lastRuntime=currentRuntime;
						String bakFile=new StringBuilder().append(reportDir)
						.append(report.getFile()).append("_").append(statTime).append(".csv").toString();
						new File(reportFile).renameTo(new File(bakFile));
						
					}
				}else{
					reportFile = new StringBuilder()
					.append(reportFile.substring(0,
							reportFile.indexOf(".csv"))).append("_")
					.append(System.currentTimeMillis()).append(".csv")
					.toString();
				}
	
			}
			
			createReportFileThreadPool.execute(
					new CreateReportOperation(reportFile,report,entryResultPool,reports,countDownLatch,outputEncoding));
			reportFiles.add(reportFile);
		}
		
		try
		{
			boolean allexport = countDownLatch.await(3, TimeUnit.MINUTES);
			
			if (!allexport)
			{
				logger.error("3 minute use,but not export all reports!");
			}
		}
		catch(Exception ex)
		{
			logger.error("generateReports error.",ex);
		}
		

		createTimeStampFile(normalDir.toString());	
		
		//清理lazy数据
		ReportUtil.cleanLazyData(entryResultPool, statisticsRule.getEntryPool());

		if (logger.isInfoEnabled())
			logger.info(new StringBuilder("generate report end")
					.append(", time consume: ")
					.append((System.currentTimeMillis() - start) / 1000)
					.toString());
		return reports;
	}
	
	
	protected void createTimeStampFile(String dir) {
		// 创建一个报表输出时间戳文件，用于增量分析
		String timeStampFile = new StringBuilder().append(dir)
				.append(AnalysisConstants.TIMESTAMP_FILE).toString();

		BufferedWriter bwr = null;

		try {
			new File(timeStampFile).createNewFile();

			bwr = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(timeStampFile))));

			bwr.write(String.valueOf(System.currentTimeMillis()));
		} catch (Exception ex) {
			logger.error("createTimeStampFile error!", ex);
		} finally {
			if (bwr != null) {
				try {
					bwr.close();
				} catch (IOException e) {
					logger.error(e, e);
				}
			}
		}

	}

	@Override
	public void exportEntryData(Job job) {
		JobDataOperation jobDataOperation = new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_EXPORTDATA,this.config);
		createReportFileThreadPool.execute(jobDataOperation);
	}


	@Override
	public void loadEntryData(Job job) {
		JobDataOperation jobDataOperation = new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_LOADDATA,this.config);
		createReportFileThreadPool.submit(jobDataOperation);
	}


	@Override
	public void loadEntryDataToTmp(Job job) {
		JobDataOperation jobDataOperation = new JobDataOperation(job,AnalysisConstants.JOBMANAGER_EVENT_LOADDATA_TO_TMP,this.config);
		createReportFileThreadPool.submit(jobDataOperation);
	}

}
