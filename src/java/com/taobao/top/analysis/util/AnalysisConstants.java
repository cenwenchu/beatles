package com.taobao.top.analysis.util;

/**
 * 报表常量定义
 * 
 * @author fangweng
 * 
 */
public class AnalysisConstants {

	public final static String IGNORE_PROCESS = "ignore";// 用于系统内部过滤不符合条件的key
	public final static String GLOBAL_KEY_STR = "GLOBAL_KEY";// 定义在报表中的全局key，作为对所有记录作全局统计用，也就是没有key做统计
	public final static String RECORD_LENGTH = "RECORD_LENGTH";// 定义在报表过滤条件中，用于过滤切割后记录数是否符合条件
	public final static Integer GLOBAL_KEY = -2;
	public final static Integer Object_KEY = -3;
	public final static String MAGIC_NUM = "^_^!";//用与定义distinct count key的关键字，防止被重复生成distinct count key
	

	// condition的条件设置
	public final static byte CONDITION_EQUAL = 0x1;
	public final static byte CONDITION_NOT_EQUAL = 0x2;
	public final static byte CONDITION_GREATER = 0x3;
	public final static byte CONDITION_LESSER = 0x4;
	public final static byte CONDITION_EQUALORGREATER = 0x5;
	public final static byte CONDITION_EQUALORLESSER = 0x6;
	public final static byte CONDITION_ISNUMBER = 0x7;
	public final static byte CONDITION_IN = 0x8;
	public final static byte CONDITION_LIKE = 0x9;
	
	
	public final static byte OPERATE_PLUS = 0x11;
	public final static byte OPERATE_MINUS = 0x12;
	public final static byte OPERATE_DIVIDE = 0x13;
	public final static byte OPERATE_RIDE = 0x14;
	
	public final static String CONDITION_EQUAL_STR = "=";
	public final static String CONDITION_NOT_EQUAL_STR = "!=";
	public final static String CONDITION_GREATER_STR = ">";
	public final static String CONDITION_LESSER_STR = "<";
	public final static String CONDITION_EQUALORGREATER_STR = ">=";
	public final static String CONDITION_EQUALORLESSER_STR = "<=";
	public final static String CONDITION_ISNUMBER_STR = "isnumber";// 是否是数字
	public final static String CONDITION_ROUND_STR = "round:";// 是否需要保留几位小数
	public final static String CONDITION_IN_STR = "in";
	public final static String CONDITION_LIKE_STR = "like";
	
	
	public final static char OPERATE_PLUS_CHAR = '+';
	public final static char OPERATE_MINUS_CHAR = '-';
	public final static char OPERATE_DIVIDE_CHAR = '/';
	public final static char OPERATE_RIDE_CHAR = '*';
	
			
	//报表中的特殊字符
	public final static String PREF_SUM = "_:s";
	public final static String PREF_COUNT = "_:c";
	
	public final static String SPLIT_KEY = "--";

	public final static String RETURN = "\r\n";
	
	public final static String REPLACE_PREFIX = ":i";
	
	
	//用于导出数据的私有分隔符
	public final static String EXPORT_RECORD_SPLIT = "<->";
	public final static String EXPORT_COLUMN_SPLIT = "(!)";
	public final static String EXPORT_DOUBLE_SPLIT = "d(!)";
	public final static String EXPORT_DATA_SPLIT = "!--linesplit--linesplit--!";
	
	
	//导出文件的后缀名	
	public final static String INNER_DATAFILE_SUFFIX = ".idata";
	public final static String IBCK_DATAFILE_SUFFIX = ".ibck";
	public final static String TEMP_MASTER_DATAFILE_SUFFIX = ".itemp";

	
	public final static String CHART_FILENAME = "topanalysis.html";
	public final static String DATA_RESULT = "topresult.data";
	public final static String DATA_JOBSTATUSPOOL = "topjobstatus.data";
	public final static String DATA_JOBS = "topjobs.data";
	public final static String TIMESTAMP_FILE = "analysis.timestamp";

	public final static String JOBFILEFROM_FTP = "ftp";
	public final static String JOBFILEFROM_MACHINE = "machine";
	
	public final static String JOBMANAGER_EVENT_LOADDATA = "loadData";
	public final static String JOBMANAGER_EVENT_LOADDATA_TO_TMP = "loadDataToTmp";
	public final static String JOBMANAGER_EVENT_EXPORTDATA = "exportData";
	public final static String JOBMANAGER_EVENT_SETNULL_EXPORTDATA = "setNullexportData";
	public final static String JOBMANAGER_EVENT_DEL_DATAFILE = "delDataFile";
	public final static String JOBMANAGER_EVENT_LOAD_BACKUPDATA = "loadBackupData";
	public final static String JOBMANAGER_EVENT_GET_SOURCETIMESTAMP = "getSourceTimeStamp";
	
	
	public final static String ANALYSIS_BLOOM_MAXKEYS = "maxKeys";
	public final static String ANALYSIS_BLOOM_ERRORRATE = "errorRate";
	
	public final static String REPORT_PERIOD_HOUR = "hour";
	public final static String REPORT_PERIOD_DAY = "day";
	public final static String REPORT_PERIOD_MONTH = "month";
	
	public final static String ZK_ROOT = "/beatles";
	public final static String ZK_MASTER = "/master";
	public final static String ZK_SLAVE = "/slave";
	public final static String ZK_CONFIG = "/config";
	public final static String ZK_LEADER_MASTER_PREFIX = "/leader:";

}
