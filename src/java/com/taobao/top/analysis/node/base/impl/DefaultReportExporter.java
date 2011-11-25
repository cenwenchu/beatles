/**
 * 
 */
package com.taobao.top.analysis.node.base.impl;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.job.JobTask;
import com.taobao.top.analysis.node.base.IReportExporter;
import com.taobao.top.analysis.statistics.data.Report;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.NamedThreadFactory;
import com.taobao.top.analysis.util.ReportUtil;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-25
 *
 */
public class DefaultReportExporter implements IReportExporter {

	private final Log logger = LogFactory.getLog(DefaultReportExporter.class);
	/**
	 * 用于输出报表文件的线程池
	 */
	private ThreadPoolExecutor createReportFileThreadPool;
	private MasterConfig masterConfig;
	private long lastRuntime=(System.currentTimeMillis() + 8 * 60 * 60 * 1000) / 86400000;
	
	@Override
	public void init() {
		createReportFileThreadPool = new ThreadPoolExecutor(
				this.masterConfig.getMaxCreateReportWorker(),
				this.masterConfig.getMaxCreateReportWorker(), 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("createReportFile_worker"));
	}

	
	@Override
	public void destory() {
		createReportFileThreadPool.shutdown();
	}

	
	@Override
	public List<String> generateReports(JobTask jobTask, boolean needTimeSuffix) {
		long start = System.currentTimeMillis();
		
		List<String> reports = new CopyOnWriteArrayList<String>();
		
		Map<String, Map<String, Object>> entryResultPool = jobTask.getResults();
		
		if (entryResultPool == null || jobTask.getStatisticsRule().getReportPool() == null
				|| (entryResultPool != null && entryResultPool.size() == 0)
				|| (jobTask.getStatisticsRule().getReportPool() != null
					&& jobTask.getStatisticsRule().getReportPool().size() == 0))
			return reports;

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

		String rootDir = jobTask.getOutput();
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

		Iterator<Report> iter = jobTask.getStatisticsRule().getReportPool()
				.values().iterator();
		
		CountDownLatch countDownLatch = new CountDownLatch(jobTask.getStatisticsRule().getReportPool().size());
		List<String> reportFiles = new ArrayList<String>();
		
		while (iter.hasNext()) {
			Report report = iter.next();
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
					new CreateReportFileTask(jobTask,reportFile,report,entryResultPool,reports,countDownLatch));
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
	
	//add by fangweng 2011 performance
	//通过不排序和增加对map访问来减少对mem的利用，但是在性能上或者速度上也许有影响
	private void createReportFile(JobTask jobTask,String reportFile,Report report,Map<String,
			Map<String, Object>> entryResultPool,List<String> reports,CountDownLatch countDownLatch)
	{
		BufferedWriter bout = null;
		boolean needTitle=false;
		try {
			File file=new File(reportFile);
			if(!file.exists()) {
				needTitle=true;
				file.createNewFile();
			}

			if(report.isAppend()){
				bout = new BufferedWriter(new java.io.OutputStreamWriter(
						new java.io.FileOutputStream(file,true),
						jobTask.getInputEncoding()));
			}else{
				bout = new BufferedWriter(new java.io.OutputStreamWriter(
						new java.io.FileOutputStream(file),
						jobTask.getInputEncoding()));
			}

			
			
			if (report.getReportEntrys() != null && report.getReportEntrys().size() > 0)
			{
				List<ReportEntry> rs = report.getReportEntrys();
				
				//输出title
				if(needTitle)
					for(int i =0 ; i < rs.size(); i++)
					{
						ReportEntry entry = report.getReportEntrys().get(i);
						bout.write(entry.getName());
						
						if (i == rs.size() -1)
							bout.write("\r\n");
						else
							bout.write(",");
						
					}
				
				//按行开始输出内容
				for(int i = 0 ; i < rs.size(); i++)
				{
					ReportEntry entry = report.getReportEntrys().get(i);
					
					Map<String, Object> m = entryResultPool.get(entry.getId());
					
					if (m == null || (m != null && m.size() == 0))
						continue;
					
					Iterator<String> iter = m.keySet().iterator();
					
					while(iter.hasNext())
					{
						String key = iter.next();
						
						// 作average的中间临时变量不处理
						if (key.startsWith(AnalysisConstants.PREF_SUM)
								|| key.startsWith(AnalysisConstants.PREF_COUNT)) {
							continue;
						}
						
						boolean needProcess = true;
						
						//判断是否前面已经有输出
						for(int j = 0; j < i; j++)
						{
							if (entryResultPool.get(report.getReportEntrys().get(j).getId())  != null
									&& entryResultPool.get(report.getReportEntrys().get(j).getId()).containsKey(key))
							{
								needProcess = false;
								break;
							}
						}
						
						if (needProcess)
						{
							for(int j = 0 ; j < i ; j++)
							{
								bout.write("0,");
							}
							
							for(int j = i ; j < rs.size(); j++)
							{
								
								ReportEntry tmpEntry = report.getReportEntrys().get(j);
								
								Object value = null;
								
								if (entryResultPool.get(tmpEntry.getId()) != null)
									value = entryResultPool.get(tmpEntry.getId()).get(key);

								if (value != null && tmpEntry.getFormatStack() != null
										&& tmpEntry.getFormatStack().size() > 0) {
									value = ReportUtil.formatValue(
											tmpEntry.getFormatStack(), value);
								}
								
								if (value != null)
								{
									if (value.toString().indexOf(",") != -1)
										bout.write("\"" + value.toString() + "\"");
									else
										bout.write(value.toString());
									
								}
								else
									bout.write("0");
								
								if (j != rs.size() -1)
									bout.write(",");
								else
									bout.write("\r\n");
								
							}
							
						}//end need process one key
						
					}//end loop one map
					
				}//end all entrys
				
				// 周期类报表就输出一次，结果将会被删除
				if (report.isPeriod()) 
				{
					for(int i = 0 ; i < rs.size(); i++)
					{
									
						Map<String, Object> _deleted = entryResultPool
								.remove(report.getReportEntrys().get(i).getId());
	
						if (_deleted != null)
							_deleted.clear();					
						
					}
				}
				
			}
					

			if (!report.isPeriod())
				reports.add(reportFile);

		} catch (Exception ex) {
			logger.error(ex, ex);
		} finally {
			countDownLatch.countDown();
			
			if (bout != null)
				try {
					bout.close();
				} catch (IOException e) {
					logger.error(e, e);
				}
				
			
		}
	}
	
	class CreateReportFileTask implements java.lang.Runnable
	{
		JobTask jobTask;
		String reportFile;
		Report report;
		Map<String,Map<String, Object>> entryResultPool;
		List<String> reports;
		CountDownLatch countDownLatch;
		
		public CreateReportFileTask(JobTask jobTask,String reportFile,Report report,Map<String,
				Map<String, Object>> entryResultPool,List<String> reports,CountDownLatch countDownLatch)
		{
			this.jobTask = jobTask;
			this.reportFile = reportFile;
			this.report = report;
			this.entryResultPool = entryResultPool;
			this.reports = reports;
			this.countDownLatch = countDownLatch;
		}
		
		@Override
		public void run() 
		{	
			createReportFile(jobTask,reportFile,report,entryResultPool,reports,countDownLatch);
		}
		
	}

}
