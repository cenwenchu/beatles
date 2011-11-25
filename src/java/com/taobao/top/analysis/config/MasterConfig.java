/**
 * 
 */
package com.taobao.top.analysis.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author fangweng
 *
 */
public class MasterConfig extends AbstractConfig{

	private static final Log logger = LogFactory.getLog(MasterConfig.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4127398444132837605L;
	
	private final static String MASTER_NAME = "masterName";
	
	private final static String MASTER_PORT = "masterPort";
	
	private final static String MAX_MERGE_JOB_WORKER = "maxMergeJobWorker";
	
	private final static String MAX_CREATE_REPORT_WORKER = "maxCreateReportWorker";
	
	/**
	 * 是否是单线程阻塞方式合并结果
	 */
	private final static String NEED_BLOCK_TO_MERGE_RESULT = "needBlockToMergeResult";

	/**
	 * 合并最小的结果数
	 */
	private final static String MIN_MERGE_JOB_COUNT ="minMergeJobCount";

	/**
	 * 设置了minMergeJobCount后最大等待组成一个bundle批量处理的时间，默认为1秒
	 */
	private final static String MAX_JOB_RESULT_BUNDLE_WAITTIME ="maxJobResultBundleWaitTime";

	/**
	 * 任何结果只允许并行合并一次，后续就必须合并到主干
	 */
	private final static String RESULT_PROCESS_ONLY_ONCE = "resultProcessOnlyOnce";
	
	/**
	 * 输出临时文件间隔
	 */
	private final static String EXPORT_INTERVAL = "exportInterval";
	
	// mod by fangweng 2011 performance
	//导出后清除对应的map内的数据，下次合并过程中再尝试从磁盘载入，通过配置开关判断是否实施
	private final static String SAVE_TMP_RESULT_TO_FILE = "saveTmpResultToFile";
	
	//mod by fangweng 2011 performance
	//配合磁盘换内存的方式，判断什么时候可以异步载入文件
	private final static String ASYN_LOAD_DISK_FILE_PRECENT = "asynLoadDiskFilePrecent";

	// mod by fangweng 2011 performance
	//是否采用私有导出模式
	private final static String USE_INNER_DATA_EXPORT = "useInnerDataExport";
	
	public String getMasterName() {
		if(this.properties.containsKey(MASTER_NAME))
			return (String)this.properties.get(MASTER_NAME);
		else
			return null;
	}

	public void setMasterName(String masterName) {
		this.properties.put(MASTER_NAME,masterName);
	}

	public int getMasterPort() {
		if(this.properties.containsKey(MASTER_PORT))
			return Integer.parseInt((String)this.properties.get(MASTER_PORT));
		else
			return 7777;
	}

	public void setMasterPort(String masterPort) {
		this.properties.put(MASTER_PORT,masterPort);
	}

	public int getMaxMergeJobWorker() {
		if(this.properties.containsKey(MAX_MERGE_JOB_WORKER))
			return Integer.parseInt((String)this.properties.get(MAX_MERGE_JOB_WORKER));
		else
			return Runtime.getRuntime().availableProcessors();
	}

	public void setMaxMergeJobWorker(String maxMergeJobWorker) {
		this.properties.put(MAX_MERGE_JOB_WORKER, maxMergeJobWorker);
	}

	public int getMaxCreateReportWorker() {
		if(this.properties.containsKey(MAX_CREATE_REPORT_WORKER))
			return Integer.parseInt((String)this.properties.get(MAX_CREATE_REPORT_WORKER));
		else
			return 2;
	}

	public void setMaxCreateReportWorker(String maxCreateReportWorker) {
		this.properties.put(MAX_CREATE_REPORT_WORKER, maxCreateReportWorker);
	}

	public boolean getNeedBlockToMergeResult() {
		if(this.properties.containsKey(NEED_BLOCK_TO_MERGE_RESULT))
			return Boolean.parseBoolean((String)this.properties.get(NEED_BLOCK_TO_MERGE_RESULT));
		else
			return false;
	}

	public void setNeedBlockToMergeResult(String needBlockToMergeResult) {
		this.properties.put(NEED_BLOCK_TO_MERGE_RESULT, needBlockToMergeResult);
	}

	public int getMinMergeJobCount() {
		if(this.properties.containsKey(MIN_MERGE_JOB_COUNT))
			return Integer.parseInt((String)this.properties.get(MIN_MERGE_JOB_COUNT));
		else
			return 1;
	}

	public void setMinMergeJobCount(String minMergeJobCount) {
		this.properties.put(MIN_MERGE_JOB_COUNT, minMergeJobCount);
	}

	public long getMaxJobResultBundleWaitTime() {
		if(this.properties.containsKey(MAX_JOB_RESULT_BUNDLE_WAITTIME))
			return Long.parseLong((String)this.properties.get(MAX_JOB_RESULT_BUNDLE_WAITTIME)) * 1000;
		else
			return 1000;
	}

	public void setMaxJobResultBundleWaitTime(String maxJobResultBundleWaitTime) {
		this.properties.put(MAX_JOB_RESULT_BUNDLE_WAITTIME,maxJobResultBundleWaitTime);
	}

	public boolean getResultProcessOnlyOnce() {
		if(this.properties.containsKey(RESULT_PROCESS_ONLY_ONCE))
			return Boolean.parseBoolean((String)this.properties.get(RESULT_PROCESS_ONLY_ONCE));
		else
			return false;
	}

	public void setResultProcessOnlyOnce(String resultProcessOnlyOnce) {
		this.properties.put(RESULT_PROCESS_ONLY_ONCE,resultProcessOnlyOnce);
	}

	public long getExportInterval() {
		if(this.properties.containsKey(EXPORT_INTERVAL))
			return Long.parseLong((String)this.properties.get(EXPORT_INTERVAL)) * 1000;
		else
			return 600 * 1000;
	}

	public void setExportInterval(String exportInterval) {
		this.properties.put(EXPORT_INTERVAL,exportInterval);
	}

	public boolean getSaveTmpResultToFile() {
		if(this.properties.containsKey(SAVE_TMP_RESULT_TO_FILE))
			return Boolean.parseBoolean((String)this.properties.get(SAVE_TMP_RESULT_TO_FILE));
		else
			return false;
	}

	public void setSaveTmpResultToFile(String saveTmpResultToFile) {
		this.properties.put(SAVE_TMP_RESULT_TO_FILE,saveTmpResultToFile);
	}

	public int getAsynLoadDiskFilePrecent() {
		if(this.properties.containsKey(ASYN_LOAD_DISK_FILE_PRECENT))
			return Integer.parseInt((String)this.properties.get(ASYN_LOAD_DISK_FILE_PRECENT));
		else
			return 90;
	}

	public void setAsynLoadDiskFilePrecent(String asynLoadDiskFilePrecent) {
		this.properties.put(ASYN_LOAD_DISK_FILE_PRECENT,asynLoadDiskFilePrecent);
	}

	public boolean getUseInnerDataExport() {
		if(this.properties.containsKey(USE_INNER_DATA_EXPORT))
			return Boolean.parseBoolean((String)this.properties.get(USE_INNER_DATA_EXPORT));
		else
			return true;
	}

	public void setUseInnerDataExport(String useInnerDataExport) {
		this.properties.put(USE_INNER_DATA_EXPORT,useInnerDataExport);
	}

}
