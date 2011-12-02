/**
 * 
 */
package com.taobao.top.analysis.node.operation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.top.analysis.statistics.data.Report;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;

/**
 * 创建报表的可执行类，用于异步化执行创建报表操作
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class CreateReportOperation implements Runnable {

	private final Log logger = LogFactory.getLog(CreateReportOperation.class);
	String reportFile;
	Report report;
	Map<String,Map<String, Object>> entryResultPool;
	List<String> reports;
	CountDownLatch countDownLatch;
	String outputEncoding;
	
	public CreateReportOperation(String reportFile,Report report,Map<String,
			Map<String, Object>> entryResultPool,List<String> reports,CountDownLatch countDownLatch,String outputEncoding)
	{
		this.reportFile = reportFile;
		this.report = report;
		this.entryResultPool = entryResultPool;
		this.reports = reports;
		this.countDownLatch = countDownLatch;
		this.outputEncoding = outputEncoding;
	}
	
	@Override
	public void run() 
	{	
		createReportFile(reportFile,report,entryResultPool,reports,countDownLatch,outputEncoding);
	}
	
	//add by fangweng 2011 performance
	//通过不排序和增加对map访问来减少对mem的利用，但是在性能上或者速度上也许有影响
	private void createReportFile(String reportFile,Report report,Map<String,
			Map<String, Object>> entryResultPool,List<String> reports,CountDownLatch countDownLatch,String outputEncoding)
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
						outputEncoding));
			}else{
				bout = new BufferedWriter(new java.io.OutputStreamWriter(
						new java.io.FileOutputStream(file),
						outputEncoding));
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

}
