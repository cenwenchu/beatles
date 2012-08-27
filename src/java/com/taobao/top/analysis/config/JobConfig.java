/**
 * 
 */
package com.taobao.top.analysis.config;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.util.AnalysisConstants;

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
	 * 报表配置文件，模型配置，用于建立job的统计规则
	 */
	private final static String REPORT_CONFIGS = "reportConfigs";
	
	/**
	 * 分析内容输入目录
	 */
	private final static String INPUT ="input";
	/**
	 * 分析结果输出目录，xxx:xxxx用冒号作为协议与具体位置的分割
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
	 * 单个任务重置时间，单位（秒），多久时间没有被执行完毕任务可以被回收再分配,默认1分钟
	 */
	private final static String TASK_RECYCLE_TIME = "taskRecycleTime";

	/**
	 * 任务组重置时间，单位（秒），默认10分钟
	 */
	private final static String JOB_RESET_TIME = "jobResetTime";
	
	/**
	 * 报表有效时间段，用于增量报表，支持day,hour,month，默认是day，也就是一天重置一次增量报表
	 */
	private final static String REPORT_PERIOD_DEFINE = "reportPeriodDefine";
	
	// mod by fangweng 2011 performance
    //数据量较小的情况下请勿使用！！！
    //导出后清除对应的map内的数据，下次合并过程中再尝试从磁盘载入，通过配置开关判断是否实施
    private final static String SAVE_TMP_RESULT_TO_FILE = "saveTmpResultToFile";
    
    //mod by fangweng 2011 performance
    //配合磁盘换内存的方式，判断什么时候可以异步载入文件
    private final static String ASYN_LOAD_DISK_FILE_PRECENT = "asynLoadDiskFilePrecent";
    
    /**
     * Hub游标每次拉取的大小
     */
    private final static String HUB_CURSOR_STEP = "hubCursorStep";
    
    /**
     * hub游标开始位置指定
     */
    private final static String HUB_CURSOR_BEGIN = "begin";
    
    /**
     * hub游标位置初始化，丢弃老数据，直接从最新文件开始
     */
    private final static String HUB_CURSOR_INIT = "init";
    
    /**
     * 由聚石塔报表所引出的特殊需求，指定不同的任务路由到不同的slave上面进行执行
     * 这里只是进行简单的配置，并没有细化到每一个task上面
     */
    private final static String SLAVE_IP_CONDITION = "slaveIp";
    
	public int getJobResetTime()
	{
		if(this.properties.containsKey(JOB_RESET_TIME))
			return Integer.parseInt((String)this.properties.get(JOB_RESET_TIME));
		else
			return 3 * 60;
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
			return 60;
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

	
	public String getReportPeriodDefine()
	{
		if (this.properties.containsKey(REPORT_PERIOD_DEFINE))
			return (String)this.properties.get(REPORT_PERIOD_DEFINE);
		else
			return AnalysisConstants.REPORT_PERIOD_DAY;
	}
	
	public void setReportPeriodDefine(String reportPeriodDefine)
	{
		this.properties.put(REPORT_PERIOD_DEFINE,reportPeriodDefine);
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
		
		if (this.properties.containsKey(SPLITREGEX)) {
		    if("space".equals(this.properties.get(SPLITREGEX)))
		        return " ";
			return (String)this.properties.get(SPLITREGEX);
		}
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
			return "GBK";
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
	
	public Boolean getSaveTmpResultToFile() {
        if(this.properties.containsKey(SAVE_TMP_RESULT_TO_FILE))
            return Boolean.parseBoolean((String)this.properties.get(SAVE_TMP_RESULT_TO_FILE));
        else
            return null;
    }

    public void setSaveTmpResultToFile(String saveTmpResultToFile) {
        this.properties.put(SAVE_TMP_RESULT_TO_FILE,saveTmpResultToFile);
    }

    public Integer getAsynLoadDiskFilePrecent() {
        if(this.properties.containsKey(ASYN_LOAD_DISK_FILE_PRECENT))
            return Integer.parseInt((String)this.properties.get(ASYN_LOAD_DISK_FILE_PRECENT));
        else
            return -1;
    }

    public void setAsynLoadDiskFilePrecent(String asynLoadDiskFilePrecent) {
        this.properties.put(ASYN_LOAD_DISK_FILE_PRECENT,asynLoadDiskFilePrecent);
    }
    
    public void setHubCursorStep(String hubCursorStep) {
        this.properties.put(HUB_CURSOR_STEP, hubCursorStep);
    }
    
    public Long getHubCursorStep() {
        if(this.properties.containsKey(HUB_CURSOR_STEP)) {
            return Long.parseLong((String)this.properties.get(HUB_CURSOR_STEP));
        }
        else
            return 3000L;
    }
    
    public void setBegin(String begin) {
        this.properties.put(HUB_CURSOR_BEGIN, begin);
    }
    
    public Long getBegin() {
        if(this.properties.containsKey(HUB_CURSOR_BEGIN)) {
            return Long.parseLong((String)this.properties.get(HUB_CURSOR_BEGIN));
        }
        return null;
    }
    
    public void setInit(String init) {
        this.properties.put(HUB_CURSOR_INIT, init);
    }
    
    public Boolean getInit() {
        if(this.properties.containsKey(HUB_CURSOR_INIT)) {
            return Boolean.parseBoolean((String)this.properties.get(HUB_CURSOR_INIT));
        }
        return false;
    }
    
    public String getSlaveIpCondition() {
        if(this.properties.containsKey(SLAVE_IP_CONDITION)) {
            return this.properties.get(SLAVE_IP_CONDITION);
        }
        return null;
    }
    
    public void setSlaveIpCondition(String slaveIpCondition) {
        this.properties.put(SLAVE_IP_CONDITION, slaveIpCondition);
    }

	@Override
	public String marshal() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void unmarshal(String content) {
		// TODO Auto-generated method stub
		
	}

}
