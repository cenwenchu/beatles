/**
 * 
 */
package com.taobao.top.analysis.config;

import org.apache.commons.lang.StringUtils;

/**
 * 任务的配置类
 * @author fangweng
 *
 */
public class JobConfig extends AbstractConfig {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9070035407756373931L;

	/**
	 * 报表配置文件，模型配置
	 */
	private final static String REPORT_CONFIGS = "reportConfigs";
	
	/**
	 * 分析内容输入目录
	 */
	private final static String INPUT ="input";
	/**
	 * 分析结果输出目录
	 */
	private final static String OUTPUT ="output";
	
	/**
	 * 对于输入分析的日志采用什么符号作为分隔符
	 */
	private final static String SPLITREGEX = "splitRegex";
	
	/**
	 * 日志文件的编码方式
	 */
	private final static String  INPUT_ENCODING = "inputEncoding";
	
	
	/**
	 * 输出文件的编码方式
	 */
	private final static String OUTPUT_ENCODING = "outputEncoding";
	
	/**
	 * 输入附加参数
	 */
	private final static String  INPUT_PARAMS = "inputParams";
	
	/**
	 * 输出附加参数
	 */
	private final static String  OUTPUT_PARAMS = "outputParams";
	
	/**
	 * 单个任务重置时间，单位（秒），多久时间没有被执行完毕任务可以被回收再分配
	 */
	private final static String TASK_RECYCLE_TIME = "taskRecycleTime";

	/**
	 * 任务组重置时间，单位（秒）
	 */
	private final static String JOB_RESET_TIME = "jobResetTime";
	
	public int getJobResetTime()
	{
		if(this.properties.containsKey(JOB_RESET_TIME))
			return Integer.parseInt((String)this.properties.get(JOB_RESET_TIME));
		else
			return 0;
	}
	
	public void setJobResetTime(String jobResetTime)
	{
		this.properties.put(JOB_RESET_TIME,jobResetTime);
	}
	
	public int getTaskRecycleTime()
	{
		if(this.properties.containsKey(TASK_RECYCLE_TIME))
			return Integer.parseInt((String)this.properties.get(TASK_RECYCLE_TIME));
		else
			return 0;
	}
	
	public void setTaskRecycleTime(String taskRecycleTime)
	{
		this.properties.put(TASK_RECYCLE_TIME,taskRecycleTime);
	}
	
	public String[] getReportConfigs() {
		
		if(this.properties.containsKey(REPORT_CONFIGS))
			return StringUtils.split((String)this.properties.get(REPORT_CONFIGS),",");
		else
			return null;
	}



	public void setReportConfigs(String reportConfigs) {
		this.properties.put(REPORT_CONFIGS,reportConfigs);
	}



	public String getInput() {
		return (String)this.properties.get(INPUT);
	}



	public void setInput(String input) {
		this.properties.put(INPUT,input);
	}



	public String getOutput() {
		return (String)this.properties.get(OUTPUT);
	}



	public void setOutput(String output) {
		this.properties.put(OUTPUT,output);
	}



	public String getSplitRegex() {
		
		if (this.properties.containsKey(SPLITREGEX))
			return (String)this.properties.get(SPLITREGEX);
		else
			return ",";
	}



	public void setSplitRegex(String splitRegex) {
		this.properties.put(SPLITREGEX,splitRegex);
	}



	public String getInputEncoding() {
		if (this.properties.containsKey(INPUT_ENCODING))
			return (String)this.properties.get(INPUT_ENCODING);
		else
			return "UTF-8";
	}



	public void setInputEncoding(String inputEncoding) {
		this.properties.put(INPUT_ENCODING,inputEncoding);
	}

	public String getOutputEncoding() {
		if (this.properties.containsKey(OUTPUT_ENCODING))
			return (String)this.properties.get(OUTPUT_ENCODING);
		else
			return "UTF-8";
	}



	public void setOutputEncoding(String outputEncoding) {
		this.properties.put(OUTPUT_ENCODING,outputEncoding);
	}


	public String getInputParams() {
		return (String)this.properties.get(INPUT_PARAMS);
	}



	public void setInputParams(String inputParams) {
		this.properties.put(INPUT_PARAMS,inputParams);
	}



	public String getOutputParams() {
		return  (String)this.properties.get(OUTPUT_PARAMS);
	}



	public void setOutputParams(String outputParams) {
		this.properties.put(OUTPUT_PARAMS,outputParams);
	}	
	

}
