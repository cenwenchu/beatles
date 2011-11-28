/**
 * 
 */
package com.taobao.top.analysis.node;

import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.ReportEntry;

/**
 * 可定制化的报表Key创建类（Ｍａｐ类）
 * 
 * @author fangweng
 * 
 */
public interface IReportMap {

	/**
	 * 创建ｋｅｙ
	 * 
	 * @param 报表ｅｎｔｒｙ定义
	 * @param 日志切割后的字符串数组
	 * @param 别名定义
	 * @param tempMapParam
	 *            有些情况下entry的mapClass对应的mapParams需要临时被修改掉，
	 *            但是涉及到entry的并发问题，所以增加此参数，如果需要临时修改mapClass对应的参数，
	 *            则传入此参数，在此参数非空的情况下每个map的实现都需要优先使用这个参数
	 * @return
	 */
	public String generateKey(ReportEntry entry, String[] contents,
			Map<String, Alias> aliasPool, String tempMapParams,List<InnerKey> innerKeyPool);
}
