package com.taobao.top.analysis.node.base;

import java.util.Map;

import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.ReportEntry;


/**
 * 可定制化的ｖａｌｕｅ生成类（Ｒｅｄｕｃｅ类）
 * 
 * @author wenchu
 * 
 */
public interface IReportReduce {

	/**
	 * @param 报表ｅｎｔｒｙ定义
	 * @param 日志切割后的字符串数组
	 * @param 别名定义
	 * @return
	 */
	public String generateValue(ReportEntry entry, String[] contents,
			Map<String, Alias> aliasPool);

}
