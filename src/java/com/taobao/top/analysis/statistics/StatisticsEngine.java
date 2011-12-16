/**
 * 
 */
package com.taobao.top.analysis.statistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.io.IInputAdaptor;
import com.taobao.top.analysis.node.io.IOutputAdaptor;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.job.JobTaskExecuteInfo;
import com.taobao.top.analysis.node.job.JobTaskResult;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.ReportUtil;
import com.taobao.top.analysis.util.Threshold;

/**
 * 默认计算引擎实现，用于分析任务
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public class StatisticsEngine implements IStatisticsEngine{
	private static final Log logger = LogFactory.getLog(StatisticsEngine.class);
	
	private Threshold threshold;
	SlaveConfig config;
	
	/**
	 * 输入的适配器，用于支持任务执行时数据来源的扩展
	 */
	List<IInputAdaptor> inputAdaptors;
	/**
	 * 输出的适配器，用于支持任务执行完毕以后数据输出的扩展
	 */
	List<IOutputAdaptor> outputAdaptors;
	
	public StatisticsEngine()
	{
		inputAdaptors = new ArrayList<IInputAdaptor>();
		outputAdaptors = new ArrayList<IOutputAdaptor>();
		threshold = new Threshold(1000);
	}
	
	@Override
	public void init() throws AnalysisException {
	}

	@Override
	public void releaseResource() {
		
	}

	@Override
	public SlaveConfig getConfig() {
		return config;
	}

	@Override
	public void setConfig(SlaveConfig config) {
		this.config = config;
	}
	
	@Override
	public void addInputAdaptor(IInputAdaptor inputAdaptor) {
		inputAdaptors.add(inputAdaptor);
	}

	@Override
	public void removeInputAdaptor(IInputAdaptor inputAdaptor) {
		inputAdaptors.remove(inputAdaptor);
	}

	@Override
	public void addOutputAdaptor(IOutputAdaptor outputAdaptor) {
		outputAdaptors.add(outputAdaptor);
	}

	@Override
	public void removeOutputAdaptor(IOutputAdaptor outputAdaptor) {
		outputAdaptors.remove(outputAdaptor);
	}
	
	@Override
	public void doExport(JobTask jobTask,JobTaskResult jobTaskResult)
	{
		for(IOutputAdaptor outputAdaptor : outputAdaptors)
		{
			if (outputAdaptor.ignore(jobTask.getOutput()))
				continue;
			
			outputAdaptor.sendResultToOutput(jobTask,jobTaskResult);
		}
	}

	@Override
	public JobTaskResult doAnalysis(JobTask jobTask) throws UnsupportedEncodingException, IOException {
		
		InputStream in = null;
		
		try
		{
			for(IInputAdaptor inputAdaptor : inputAdaptors)
			{
				if (inputAdaptor.ignore(jobTask.getInput()))
					continue;
				
				in = inputAdaptor.getInputFormJob(jobTask);
				
				if (in != null)
					break;
			}
			
			if (in == null)
			{
				throw new IOException("Input not found! input : " + jobTask.getInput());
			}
			
			return analysis(in,jobTask);
		}
		finally
		{
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error(e,e);
				}
		}

	}
	
	// 分析数据
	JobTaskResult analysis(InputStream in,JobTask jobtask) throws UnsupportedEncodingException
	{
		
		String encoding = jobtask.getInputEncoding();
		String splitRegex = jobtask.getSplitRegex();
		
		JobTaskResult jobTaskResult = new JobTaskResult();
		jobTaskResult.addTaskId(jobtask.getTaskId());
		jobTaskResult.setCreatTime(jobtask.getCreatTime());
		
		JobTaskExecuteInfo taskExecuteInfo = new JobTaskExecuteInfo();
		jobTaskResult.addTaskExecuteInfo(taskExecuteInfo);
		
		Map<String, ReportEntry> entryPool = jobtask.getStatisticsRule().getEntryPool();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(in, encoding));
		
		int normalLine = 0;//正常的行数
		int emptyLine=0;//拉取空行的次数
		int exceptionLine=0;//一行中，只要entry有异常，该行就是存在异常的行。
		int size = 0;
		String record;
		
		ReportEntry entry = null;
		
		long beg = System.currentTimeMillis();
		
		try 
		{
			//逐行处理
			while ((record = reader.readLine()) != null) 
			{
				boolean failure=false;
				
				try
				{
					if (record == null || "".equals(record)) 
					{
						emptyLine++;
						continue;
					}
					
					size += record.length();
					
					String[] contents = StringUtils.splitByWholeSeparator(record, splitRegex);
					Iterator<String> keys = entryPool.keySet().iterator();
					while (keys.hasNext()) 
					{
						try 
						{
							String key = keys.next();
							entry = entryPool.get(key);
							if(!entry.isLazy()){
								processSingleLine(entry, contents,jobtask,jobTaskResult);
							}
							
						} 
						catch (Throwable e) 
						{
							if(!failure) 
								exceptionLine++;
							
							failure=true;
							
							if (!threshold.sholdBlock())
								logger.error(new StringBuilder().append("Entry :")
									.append(entry.getId()).append("\r\n record: ")
									.append(record).toString(), e);
						}
					}

					if(!failure) 
						normalLine++;
					
				}
				catch(Throwable t)
				{
					if(!failure) 
						exceptionLine++;
					
					if (!threshold.sholdBlock())
						logger.error(
							new StringBuilder()
									.append("\r\n record: ").append(record)
									.toString(), t);
				}
			}
			
		}
		catch (Throwable ex) {
			taskExecuteInfo.setSuccess(false);
			logger.error(ex,ex);
		} 
		finally 
		{
			if (reader != null) 
			{
				try {
					reader.close();
					reader = null;
				} 
				catch (Throwable ex) {
					logger.error(ex,ex);
				}
			}
			System.out.println("####"+(System.currentTimeMillis() - beg));
			taskExecuteInfo.setAnalysisConsume(System.currentTimeMillis() - beg);
			taskExecuteInfo.setEmptyLine(emptyLine);
			taskExecuteInfo.setErrorLine(exceptionLine);
			taskExecuteInfo.setJobDataSize(size*2);
			taskExecuteInfo.setTotalLine(normalLine+exceptionLine+emptyLine);
			taskExecuteInfo.setWorkerIp(ReportUtil.getIp());
			
			if (logger.isWarnEnabled())
				logger.warn(new StringBuilder("jobtask ").append(jobtask.getTaskId())
					.append(",normal line count: ").append(normalLine)
					.append(",exception line count:").append(exceptionLine)
					.append(",empty line:").append(emptyLine).toString());
		}
		
		return jobTaskResult;
		
	}
	
	//处理单行数据
	public void processSingleLine(ReportEntry entry,String[] contents,JobTask jobtask,JobTaskResult jobTaskResult){
		Map<String, Map<String, Object>> entryResult = jobTaskResult.getResults();
		String key = entry.getMapClass().mapperKey(entry,contents, jobtask);
		if(key != null){
			Object value = entry.getMapClass().mapperValue(entry, contents, jobtask);
			Map<String,Object> result = entryResult.get(entry.getId());
			if(result == null){
				result = new HashMap<String, Object>();
				jobTaskResult.getResults().put(entry.getId(), result);
			}
			entry.getReduceClass().reducer(entry,key,value,result);
		}
		
	}

}
