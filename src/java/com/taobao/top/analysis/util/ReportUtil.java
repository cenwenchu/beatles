package com.taobao.top.analysis.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.EntryValueOperator;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.Report;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.data.ReportOrderComparator;


/**
 * 报表工具类
 * 
 * @author fangweng
 * 
 */
public class ReportUtil {
	private static final Log logger = LogFactory.getLog(ReportUtil.class);
	private static Map<Object, Object> localCache = new ConcurrentHashMap<Object, Object>();
	
	private static String ip;
	
	private static String separator;
	
	static 
	{
		try 
		{
			ip = InetAddress.getLocalHost().getHostAddress();
			separator = System.getProperty("line.separator");
		} 
		catch (UnknownHostException e) {
		}
	}
	
	
	
	public static String getIp() {
		return ip;
	}

	public static String getSeparator() {
		return separator;
	}

	public static InputStream getInputStreamFromFile(String file) throws IOException
	{
		InputStream in = null;
		
		String localdir = new StringBuilder()
				.append(System.getProperty("user.dir"))
					.append(File.separatorChar).toString();
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		
		if (file.startsWith("file:")) {
			try {
				in = new java.io.FileInputStream(new File(
						file.substring(file.indexOf("file:")
								+ "file:".length())));
			} catch (Exception e) {
				logger.error(e);
			}

			if (in == null)
				in = new java.io.FileInputStream(new File(localdir
						+ file.substring(file.indexOf("file:")
								+ "file:".length())));
		} else {
			URL url = loader.getResource(file);

			if (url == null) {
				String error = "configFile: " + file + " not exist !";
				logger.error(error);
				throw new java.lang.RuntimeException(error);
			}

			in = url.openStream();
		}
		
		return in;
	}
	
	/**
	 * 根据定义获取对应日志行产生的key
	 * 
	 * @param entry
	 * @param contents
	 * @return
	 */
	public static String generateKey(ReportEntry entry, String[] contents,List<InnerKey> innerKeyPool) {
		StringBuilder key = new StringBuilder();

		try {
			boolean checkResult = false;

			if (entry.getConditionKStack() != null
					&& entry.getConditionKStack().size() > 0) {
				for (int i = 0; i < entry.getConditionKStack().size(); i++) {

					Object conditionKey = entry.getConditionKStack().get(i);
					String operator = entry.getConditionOpStack().get(i);
					String conditionValue = entry.getConditionVStack().get(i);
					int k = -1;

					// 长度condition特殊处理，没有指定的key列
					if (!conditionKey.equals(AnalysisConstants.RECORD_LENGTH)) {
						k = (Integer) conditionKey;
					}

					checkResult = checkKeyCondition(operator, k,
							conditionValue, contents);

					if (entry.isAndCondition() && !checkResult)
						return AnalysisConstants.IGNORE_PROCESS;

					if (!entry.isAndCondition() && checkResult)
						break;
				}
			}

			if (!entry.isAndCondition() && !checkResult)
				return AnalysisConstants.IGNORE_PROCESS;

			for (String c : entry.getKeys()) {
				// 全局统计，没有key
				if (c.equals(AnalysisConstants.GLOBAL_KEY))
					return AnalysisConstants.GLOBAL_KEY;

				key.append(innerKeyReplace(c,contents[Integer.valueOf(c) - 1],innerKeyPool)).append(AnalysisConstants.SPLIT_KEY);
			}

		} catch (Exception ex) {
			return AnalysisConstants.IGNORE_PROCESS;
		}

		return key.toString();
	}
	
	private static String innerKeyReplace(String key,String value,List<InnerKey> innerKeyPool)
	{
		String result = value;
		
		if (innerKeyPool == null || (innerKeyPool != null && innerKeyPool.size() == 0))
			return result;
		
		for(InnerKey ik : innerKeyPool)
		{
			if (ik.getKey().equals(key))
			{
				if (ik.getInnerKeys().get(value) != null)
					result = ik.getInnerKeys().get(value);
				
				break;
			}
		}
		
		return result;
	}
	
	/**
	 * 返回是否符合条件
	 * 
	 * @param operator
	 * @param conditionKey
	 * @param conditionValue
	 * @param contents
	 * @return
	 */
	private static boolean checkKeyCondition(String operator, int conditionKey,
			String conditionValue, String[] contents) {
		boolean result = false;

		if (operator.equals(AnalysisConstants.CONDITION_EQUAL)) {
			if (conditionKey > 0)
				result = contents[conditionKey - 1].equals(conditionValue);
			else
				result = contents.length == Integer.valueOf(conditionValue);
		} else if (operator.equals(AnalysisConstants.CONDITION_NOT_EQUAL)) {
			if (conditionKey > 0)
				result = !contents[conditionKey - 1].equals(conditionValue);
			else
				result = contents.length != Integer.valueOf(conditionValue);
		} else {
			double cmpValue = 0;

			if (conditionKey > 0)
				cmpValue = Double.valueOf(contents[conditionKey - 1])
						- Double.valueOf(conditionValue);
			else
				cmpValue = contents.length - Integer.valueOf(conditionValue);

			if (operator.equals(AnalysisConstants.CONDITION_EQUALORGREATER))
				return cmpValue >= 0;

			if (operator.equals(AnalysisConstants.CONDITION_EQUALORLESSER))
				return cmpValue <= 0;

			if (operator.equals(AnalysisConstants.CONDITION_GREATER))
				return cmpValue > 0;

			if (operator.equals(AnalysisConstants.CONDITION_LESSER))
				return cmpValue < 0;

		}

		return result;
	}
	
	
	private static java.text.DateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String formateDateTime(long time){
		java.util.Calendar cal=java.util.Calendar.getInstance();
		cal.setTimeInMillis(time);
		return df.format(cal.getTime());
	}
	/**
	 * 根据别名定义来转换报表模型中定义的key，直接转换为实际的列号
	 * 
	 * @param keys
	 * @param aliasPool
	 */
	public static void transformVars(String[] keys, Map<String, Alias> aliasPool) {
		if (aliasPool != null && aliasPool.size() > 0 && keys != null
				&& keys.length > 0) {
			for (int i = 0; i < keys.length; i++) {
				if (aliasPool.get(keys[i]) != null)
					keys[i] = aliasPool.get(keys[i]).getKey();
			}
		}
	}

	/**
	 * 根据别名定义来转换报表模型中定义的key，直接转换为实际的列号
	 * 
	 * @param key
	 * @param aliasPool
	 * @return
	 */
	public static String transformVar(String key, Map<String, Alias> aliasPool) {
		String result = key;

		if (aliasPool != null && aliasPool.size() > 0
				&& aliasPool.get(key) != null) {
			result = aliasPool.get(key).getKey();
		}

		return result;
	}

	
	
	

	

	/**
	 * 根据接口定义获取实际的接口实现实例
	 * 
	 * @param <I>
	 * @param interfaceDefinition
	 * @param classLoader
	 * @param className
	 * @param needCache
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <I> I getInstance(Class<I> interfaceDefinition,
			ClassLoader classLoader, String className, boolean needCache) {

		// 获取缓存的情况，或者移除缓存
		if (needCache) {
			Object instance = localCache.get(className);

			if (instance == null) {
				instance = newInstance(interfaceDefinition, className,
						classLoader);
				localCache.put(className, instance);
			}

			return (I) instance;
		} else {
			return newInstance(interfaceDefinition, className, classLoader);
		}

	}

	/**
	 * 创建实例
	 * 
	 * @param <I>
	 * @param interfaceDefinition
	 * @param className
	 * @param classLoader
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static <I> I newInstance(Class<I> interfaceDefinition,
			String className, ClassLoader classLoader) {
		try {
			Class<I> spiClass;

			if (classLoader == null) {
				spiClass = (Class<I>) Class.forName(className);
			} else {
				spiClass = (Class<I>) classLoader.loadClass(className);
			}

			return spiClass.newInstance();
		} catch (ClassNotFoundException x) {
			throw new java.lang.RuntimeException("Provider " + className
					+ " not found", x);
		} catch (Exception ex) {
			throw new java.lang.RuntimeException("Provider " + className
					+ " could not be instantiated: " + ex, ex);
		}
	}

	/**
	 * 排序
	 * 
	 * @param list
	 * @param orders
	 */
	public static void doOrder(ArrayList<Object[]> list, String[] orders,
			Report report) {
		if (orders == null || (orders != null && (orders.length == 0)))
			return;

		int[] columns = new int[orders.length];
		boolean[] isDesc = new boolean[orders.length];

		for (int i = 0; i < isDesc.length; i++) {
			isDesc[i] = true;
		}

		int index = 0;

		for (String order : orders) {
			if (order.startsWith("+") || order.startsWith("-")) {
				if (order.startsWith("+"))
					isDesc[index] = false;

				order = order.substring(1);
			}

			for (ReportEntry entry : report.getReportEntrys()) {
				if (order.equals(entry.getName())) {
					break;
				}
				columns[index] += 1;
			}

			if (columns[index] >= report.getReportEntrys().size())
				columns[index] = 0;

			index += 1;
		}

		Collections.sort(list, new ReportOrderComparator<Object[]>(columns,
				isDesc));

	}

	/**
	 * 格式化结果,很消耗...
	 * 
	 * @param formatStack
	 * @param value
	 * @return
	 */
	public static Object formatValue(List<String> formatStack, Object value) {
		Object result = value;

		try {
			for (String filter : formatStack) {
				if (filter.startsWith(AnalysisConstants.CONDITION_ROUND)) {

					int round = Integer.valueOf(filter
							.substring(AnalysisConstants.CONDITION_ROUND
									.length()));
					double r = Math.pow(10, round);

					if (value instanceof Double) {
						result = (Double) (Math.round((Double) value * r) / r);
					} else
						result = (Double) (Math.round((Double.valueOf(value
								.toString()) * r)) / r);

					continue;
				}
			}
		} catch (Exception ex) {
			logger.error(ex, ex);
		}

		return result;
	}

	/**
	 * 检查参数是否符合过滤器定义
	 * 
	 * @param valuefilterOpStack
	 * @param valuefilterStack
	 * @param value
	 * @return
	 */
	public static boolean checkValue(List<String> valuefilterOpStack,
			List<String> valuefilterStack, Object value) {
		boolean result = true;

		if (valuefilterStack == null
				|| (valuefilterStack != null && valuefilterStack.size() == 0))
			return result;

		try {
			for (int i = 0; i < valuefilterStack.size(); i++) {
				String filterValue = valuefilterStack.get(i);
				String filterOpt = valuefilterOpStack.get(i);

				if (filterOpt.equals(AnalysisConstants.CONDITION_ISNUMBER)) {
					Double.valueOf(value.toString());
					continue;
				}

				if (filterOpt.equals(AnalysisConstants.CONDITION_EQUAL)) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = Double.valueOf(filterValue);

					if (v == compareValue) {
						continue;
					} else
						return false;
				}

				if (filterOpt
						.equals(AnalysisConstants.CONDITION_EQUALORGREATER)) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = Double.valueOf(filterValue);

					if (v >= compareValue) {
						continue;
					} else
						return false;
				}

				if (filterOpt.equals(AnalysisConstants.CONDITION_EQUALORLESSER)) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = Double.valueOf(filterValue);

					if (v <= compareValue) {
						continue;
					} else
						return false;
				}

				if (filterOpt.equals(AnalysisConstants.CONDITION_GREATER)) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = Double.valueOf(filterValue);

					if (v > compareValue) {
						continue;
					} else
						return false;
				}

				if (filterOpt.equals(AnalysisConstants.CONDITION_LESSER)) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = Double.valueOf(filterValue);

					if (v < compareValue) {
						continue;
					} else
						return false;
				}

				if (filterOpt.equals(AnalysisConstants.CONDITION_NOT_EQUAL)) {
					Double v = Double.valueOf(value.toString());
					Double compareValue = Double.valueOf(filterValue);

					if (!v.equals(compareValue)) {
						continue;
					} else
						return false;
				}

			}
		} catch (Exception ex) {
			result = false;
		}

		return result;
	}



	

	/**
	 * 合并结果集
	 * 
	 * @param resultPools
	 * @param entryPool
	 * @return
	 */
	public static Map<String, Map<String, Object>> mergeEntryResult(
			Map<String, Map<String, Object>>[] resultPools,
			Map<String, ReportEntry> entryPool, boolean needMergeLazy) {
		if (resultPools == null
				|| (resultPools != null && resultPools.length == 0))
			return null;

		Map<String, Map<String, Object>> result = null;

		result = merge(resultPools, entryPool);

		if (result == null || (result != null && result.size() <= 0))
			return result;

		if (needMergeLazy) {
			lazyMerge(result, entryPool);
		}

		return result;
	}
	
	public static void cleanLazyData(Map<String, Map<String, Object>> result,
			Map<String, ReportEntry> entryPool)
	{
		if (entryPool != null)
		{
			Iterator<String> entryKeys = entryPool.keySet().iterator();
			
			while(entryKeys.hasNext())
			{
				String entryId = entryKeys.next();
				
				ReportEntry entry = entryPool.get(entryId);
				
				if (entry.isLazy())
				{
					Map<String, Object> t = result.remove(entryId);
					
					if(t != null)
						t.clear();
				}
				
			}
		}
		
	}

	public static void lazyMerge(Map<String, Map<String, Object>> result,
			Map<String, ReportEntry> entryPool) {
		// 增加对于lazy entry的处理
		ArrayList<String> entryKeys = new ArrayList<String>();
		entryKeys.addAll(entryPool.keySet());
		// 二级lazy的优先顺序保证
		Collections.sort(entryKeys);

		for (String entryId : entryKeys) {

			ReportEntry entry = entryPool.get(entryId);

			if (entry.isLazy()) {
				if (result.get(entryId) == null)
					result.put(entryId, new HashMap<String, Object>());

				if (entry.getBindingStack() != null
						&& entry.getBindingStack().size() > 0) {
					List<String> bindingStack = entry.getBindingStack();
					int size = bindingStack.size();

					String leftEntryId = bindingStack.get(0);

					Map<String, Object> leftMap = result.get(leftEntryId);

					if (leftMap == null|| (leftMap != null && leftMap.size() <= 0)){
						continue;
					}
					Iterator<String> iter = leftMap.keySet().iterator();
					java.util.Map<String, Double> cacheMap = new java.util.HashMap<String, Double>();
					while (iter.hasNext()) {
						try {
							String nodekey = iter.next();
							Object nodevalue = result.get(leftEntryId).get(nodekey);
							Object rightvalue;

							for (int i = 0; i < size - 1; i++) {
								String rightkey = bindingStack.get(i + 1);

								if (rightkey.startsWith("sum:")) {
									rightkey = rightkey.substring(rightkey
											.indexOf("sum:") + "sum:".length());
									double sumValue = 0;
									if (cacheMap.get(rightkey) != null) {
										sumValue = cacheMap.get(rightkey);

									} else {
										Iterator<Object> rValues = result
												.get(rightkey).values()
												.iterator();

										while (rValues.hasNext()) {
											Object rv = rValues.next();

											if (rv != null) {
												if (rv instanceof String)
													sumValue += Double
															.valueOf((String) rv);
												else {
													sumValue += (Double) rv;
												}
											}

										}
										cacheMap.put(rightkey, sumValue);
									}

									rightvalue = sumValue;

								} else {
									// 简单实现只支持两位计算，同时是+或者-
									if (rightkey
											.indexOf(EntryValueOperator.PLUS
													.toString()) > 0
											|| rightkey
													.indexOf(EntryValueOperator.MINUS
															.toString()) > 0) {

										String l;
										String r;

										if (rightkey
												.indexOf(EntryValueOperator.PLUS
														.toString()) > 0) {
											l = rightkey
													.substring(
															0,
															rightkey.indexOf(EntryValueOperator.PLUS
																	.toString()))
													.trim();

											r = rightkey
													.substring(
															rightkey.indexOf(EntryValueOperator.PLUS
																	.toString()) + 1)
													.trim();

											if (result.get(l) == null
													|| result.get(r) == null
													|| (result.get(l) != null && result
															.get(l)
															.get(nodekey) == null)
													|| (result.get(r) != null && result
															.get(r)
															.get(nodekey) == null))
												continue;

											rightvalue = Double.valueOf(result
													.get(l).get(nodekey)
													.toString())
													+ Double.valueOf(result
															.get(r)
															.get(nodekey)
															.toString());
										} else {
											l = rightkey
													.substring(
															0,
															rightkey.indexOf(EntryValueOperator.MINUS
																	.toString()))
													.trim();

											r = rightkey
													.substring(
															rightkey.indexOf(EntryValueOperator.MINUS
																	.toString()) + 1)
													.trim();

											if (result.get(l) == null
													|| result.get(r) == null
													|| (result.get(l) != null && result
															.get(l)
															.get(nodekey) == null)
													|| (result.get(r) != null && result
															.get(r)
															.get(nodekey) == null))
												continue;

											rightvalue = Double.valueOf(result
													.get(l).get(nodekey)
													.toString())
													- Double.valueOf(result
															.get(r)
															.get(nodekey)
															.toString());
										}

									} else
										rightvalue = result.get(rightkey).get(
												nodekey);
								}

								if (rightvalue != null) {
									if(nodevalue!=null){
										if (entry
												.getOperatorStack()
												.get(i)
												.equals(EntryValueOperator.PLUS
														.toString())) {
											if (nodevalue instanceof Double
													|| rightvalue instanceof Double)
												nodevalue = Double
														.valueOf(nodevalue
																.toString())
														+ Double.valueOf(rightvalue
																.toString());
											else
												nodevalue = (Long) nodevalue
														+ (Long) rightvalue;

											continue;
										}

										if (entry
												.getOperatorStack()
												.get(i)
												.equals(EntryValueOperator.MINUS
														.toString())) {
											if (nodevalue instanceof Double
													|| rightvalue instanceof Double)
												nodevalue = Double
														.valueOf(nodevalue
																.toString())
														- Double.valueOf(rightvalue
																.toString());
											else
												nodevalue = (Long) nodevalue
														- (Long) rightvalue;

											continue;
										}

										if (entry
												.getOperatorStack()
												.get(i)
												.equals(EntryValueOperator.RIDE
														.toString())) {
											if (nodevalue instanceof Double
													|| rightvalue instanceof Double)
												nodevalue = Double
														.valueOf(nodevalue
																.toString())
														* Double.valueOf(rightvalue
																.toString());
											else
												nodevalue = (Long) nodevalue
														* (Long) rightvalue;

											continue;
										}

										if (entry
												.getOperatorStack()
												.get(i)
												.equals(EntryValueOperator.DIVIDE
														.toString())) {
											nodevalue = Double.valueOf(nodevalue
													.toString())
													/ Double.valueOf(rightvalue
															.toString());

											continue;
										}
									}else{
										//nodevalue=rightvalue;
									}
									

								}

							}

							result.get(entryId).put(nodekey, nodevalue);
						} catch (Exception ex) {
						
							logger.error(
									new StringBuilder("entry : ").append(
											entry.getName()).append(
											" lazy process error!"), ex);
							continue;
						}

					}
					cacheMap.clear();
				}

			}

		}
	}

	protected static Map<String, Map<String, Object>> merge(
			Map<String, Map<String, Object>>[] entryPools,
			Map<String, ReportEntry> entryConfig) {
		Map<String, Map<String, Object>> result = null;

		int _index = 0;

		// 过滤一下最前面的空的结果
		for (int i = 0; i < entryPools.length; i++) 
		{	
			_index = i;
			
			if (entryPools[i] != null) {
				result = entryPools[i];			
				break;
			}
		}


		// 直接用resultPools的第一个作为基础数组，从第二个数组开始
		for (int i = _index; i < entryPools.length; i++) {
			Map<String, Map<String, Object>> node = entryPools[i];
			if (node == null || (node != null && node.size() == 0))
				continue;
			Iterator<String> iter = node.keySet().iterator();
			while (iter.hasNext()) {
				String entryId = iter.next();
				ReportEntry entry = entryConfig.get(entryId);
				if (entry == null || (entry != null && entry.isLazy()))
					continue;
				if (result.get(entryId) == null)
					result.put(entryId, new HashMap<String, Object>());
				Map<String, Object> content = node.get(entryId);
				Iterator<String> keyIter = content.keySet().iterator();
				while (keyIter.hasNext()) {
					String key = keyIter.next();
					Object value = content.get(key);
					if (key == null || value == null)
						continue;
					try {
						switch (entry.getValueType()) {
						case AVERAGE:
							if (key.startsWith(AnalysisConstants.PREF_SUM)
									|| key.startsWith(AnalysisConstants.PREF_COUNT))
								continue;
							String sumkey = new StringBuilder().append(AnalysisConstants.PREF_SUM)
									.append(key).toString();
							String countkey = new StringBuilder()
									.append(AnalysisConstants.PREF_COUNT).append(key).toString();

							if (content.get(sumkey) != null
									&& content.get(countkey) != null) {
								double sum = 0;
								double count = 0;
								try {
									sum = Double.parseDouble(content
											.get(sumkey).toString());
									count = Double.parseDouble(content.get(
											countkey).toString());
								} catch (Throwable t) {
									
									logger.error("reportUtil merge:operator:"
											+ entry.getValueType() + ",key:"
											+ key + ",value:" + value, t);
								}

								for (int j = i + 1; j < entryPools.length; j++) {
									if (entryPools[j] != null
											&& entryPools[j].get(entryId) != null
											&& entryPools[j].get(entryId).get(
													key) != null) {
										try {
											if (entryPools[j]
															.get(entryId)
															.get(countkey) == null)
											{
												throw new java.lang.RuntimeException("merge error: key exist,but countkey not exist");
											}
											
											count += Double
													.parseDouble(entryPools[j]
															.get(entryId)
															.get(countkey)
															.toString());
											sum += Double
													.parseDouble(entryPools[j]
															.get(entryId)
															.get(sumkey)
															.toString());
										} catch (Throwable t) {
											
											logger.error(
													"reportUtil merge:operator:"
															+ entry.getValueType()
															+ ",key:" + key
															+ ",value:" + value,
													t);
										}
										entryPools[j].get(entryId).remove(key);
										entryPools[j].get(entryId).remove(
												countkey);
										entryPools[j].get(entryId).remove(
												sumkey);
									}
								}

								if (count != 0) {
									Double newvalue = sum / count;
									result.get(entryId).put(key, newvalue);
									result.get(entryId).put(sumkey, sum);
									result.get(entryId).put(countkey, count);
								}
							}
							break;
						case MIN:
							double min = Double.parseDouble(value.toString());
							for (int j = i + 1; j < entryPools.length; j++) {
								if (entryPools[j] != null
										&& entryPools[j].get(entryId) != null
										&& entryPools[j].get(entryId).get(key) != null) {
									double temp;
									try {
										temp = Double.parseDouble(entryPools[j]
												.get(entryId).get(key)
												.toString());
										if (temp < min)
											min = temp;
									} catch (Throwable t) {
										
										logger.error(
												"reportUtil merge:operator:"
														+ entry.getValueType()
														+ ",key:" + key
														+ ",value:" + value, t);
									}
									entryPools[j].get(entryId).remove(key);
								}
							}
							result.get(entryId).put(key, min);
							break;
						case MAX:
							double max = Double.parseDouble(value.toString());
							for (int j = i + 1; j < entryPools.length; j++) {
								if (entryPools[j] != null
										&& entryPools[j].get(entryId) != null
										&& entryPools[j].get(entryId).get(key) != null) {
									double temp;
									try {
										temp = Double.parseDouble(entryPools[j]
												.get(entryId).get(key)
												.toString());
										if (temp > max)
											max = temp;
									} catch (Throwable t) {
										
										logger.error(
												"reportUtil merge:operator:"
														+ entry.getValueType()
														+ ",key:" + key
														+ ",value:" + value, t);
									}
									entryPools[j].get(entryId).remove(key);
								}
							}
							result.get(entryId).put(key, max);
							break;
						case SUM:
							double sum = Double.parseDouble(value.toString());
							for (int j = i + 1; j < entryPools.length; j++) {
								if (entryPools[j] != null
										&& entryPools[j].get(entryId) != null
										&& entryPools[j].get(entryId).get(key) != null) {
									double temp;
									try {
										temp = Double.parseDouble(entryPools[j]
												.get(entryId).get(key)
												.toString());
										sum += temp;
									} catch (Throwable t) {
										
										logger.error(
												"reportUtil merge:operator:"
														+ entry.getValueType()
														+ ",key:" + key
														+ ",value:" + value, t);
									}
									entryPools[j].get(entryId).remove(key);
								}
							}
							result.get(entryId).put(key, sum);
							break;
						case COUNT:
							double count = Double.parseDouble(value.toString());
							for (int j = i + 1; j < entryPools.length; j++) {
								if (entryPools[j] != null
										&& entryPools[j].get(entryId) != null
										&& entryPools[j].get(entryId).get(key) != null) {
									double temp;
									try {
										temp = Double.parseDouble(entryPools[j]
												.get(entryId).get(key)
												.toString());
										count += temp;
									} catch (Throwable t) {
										
										logger.error(
												"reportUtil merge:operator:"
														+ entry.getValueType()
														+ ",key:" + key
														+ ",value:" + value, t);
									}
									entryPools[j].get(entryId).remove(key);
								}
							}
							result.get(entryId).put(key, count);
							break;
						case PLAIN:
							Object v = value;
							for (int j = i + 1; j < entryPools.length; j++) {
								if (entryPools[j] != null
										&& entryPools[j].get(entryId) != null
										&& entryPools[j].get(entryId).get(key) != null) {
									v = entryPools[j].get(entryId).get(key);
									entryPools[j].get(entryId).remove(key);
								}
							}
							result.get(entryId).put(key, v);
							break;
						}
					} catch (Throwable t) {
						
						logger.error(
								"reportUtil merge:operator:"
										+ entry.getValueType() + ",key:" + key
										+ ",value:" + value, t);
					}
				}

			}

		}

		return result;
	}

	/**
	 * 获得报表文件存储路径
	 * 
	 * @param 报表名称
	 * @param 输出目录
	 * @param 报表日期
	 * @param 是否需要有后缀
	 * @return
	 */
	public static String getReportFileLocation(String reportname,
			String targetDir, long date, boolean needsuffix) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(date);
		String currentTime = new StringBuilder()
				.append(calendar.get(Calendar.YEAR)).append("-")
				.append(calendar.get(Calendar.MONTH) + 1).append("-")
				.append(calendar.get(Calendar.DAY_OF_MONTH)).toString();

		StringBuilder result = new StringBuilder();

		if (!targetDir.endsWith(File.separator))
			result.append(targetDir).append(File.separator).append(currentTime)
					.append(File.separator).toString();
		else
			result.append(targetDir).append(currentTime).append(File.separator)
					.toString();

		if (needsuffix) {
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			String stattime = new StringBuilder()
					.append(calendar.get(Calendar.YEAR)).append("-")
					.append(calendar.get(Calendar.MONTH) + 1).append("-")
					.append(calendar.get(Calendar.DAY_OF_MONTH)).toString();

			result.append(reportname).append("_").append(stattime)
					.append(".csv").toString();
		} else {
			result.append(reportname).append(".csv").toString();
		}

		return result.toString();
	}

	/**
	 * @param inputFile
	 * @return
	 */
	public static String createReportHtml(String inputFile, String title,
			int countNum) {
		StringBuilder result = new StringBuilder();
		java.io.BufferedReader br = null;

		try {
			File file = new File(inputFile);

			if (!file.exists())
				throw new java.lang.RuntimeException("chart file not exist : "
						+ inputFile);

			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					file), "gb2312"));

			String line = null;
			int index = 0;

			result.append("<br/>").append(title).append("<br/>");

			while ((line = br.readLine()) != null) {
				String[] contents = line.split(",");
				if (index == 0) {
					result.append("<table border=\"1\">");
				}
				result.append("<tr>");

				for (String c : contents) {
					if (index == 0)
						result.append("<th>").append(c).append("</th>");
					else
						result.append("<td>").append(c).append("</td>");
				}

				result.append("</tr>");
				index += 1;

				if (countNum > 0 && index > countNum)
					break;
			}

			result.append("</table>");
		} catch (Exception ex) {
			
			logger.error(ex, ex);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					
					logger.error(e, e);
				}
			}
		}

		return result.toString();
	}

	/**
	 * 将对象写入文件
	 * 
	 * @param o
	 * @param file
	 */
	public static void writeObjectToFile(Object o, String file) {
		java.io.ObjectOutputStream out = null;

		try {
			new File(file).createNewFile();
			File f = new File(file);
			out = new ObjectOutputStream(new FileOutputStream(f));
			out.writeObject(o);
		} catch (Exception ex) {
			
			logger.error(ex, ex);
		} finally {
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {
					
					logger.error(e, e);
				}
		}

	}

	/**
	 * 读取对象从文件
	 * 
	 * @param file
	 * @return
	 */
	public static Object readObjectFromFile(String file) {
		Object result = null;

		ObjectInputStream bin = null;

		try {
			File f = new File(file);
			bin = new ObjectInputStream(new FileInputStream(f));
			result = bin.readObject();
		} catch (Exception ex) {
			
			logger.error(ex, ex);
		} finally {
			if (bin != null)
				try {
					bin.close();
				} catch (IOException e) {
					
					logger.error(e, e);
				}
		}

		return result;
	}

	
	//用于不可逆压缩字符串的字典
		static String[] dict = new String[]{          
	            "a","b","c","d","e","f","g","h", 
	            "i","j","k","l","m","n","o","p", 
	            "q","r","s","t","u","v","w","x", 
	            "y","z","0","1","2","3","4","5", 
	            "6","7","8","9","A","B","C","D", 
	            "E","F","G","H","I","J","K","L", 
	            "M","N","O","P","Q","R","S","T", 
	            "U","V","W","X","Y","Z","<",">"
	        }; 
		
		/**
		 * 不可逆方式压缩字符串，存在一定重复的可能性，指定的compressedLength越小，越容易重复，compress当前最大长度限制为10
		 * md5后的16byte做压缩,得到指定的压缩长度
		 * 然后再将中间字母和长度拼接在压缩字符串上
		 * @param origin
		 * @param compressedLength,需要压缩到多少位，小于等于20,当等于0的时候，判断原始字符串是否大于32,如果是，则直接用md5转换为Hex串
		 * @return
		 */
		public static String compressString(String origin,int compressedLength)
		{
			//特殊处理0
			if (compressedLength == 0 && (origin != null && origin.length() > 32))
			{
				return DigestUtils.md5Hex(origin);
			}
			
			
			if (origin ==  null || (origin != null && 
					(origin.length() < compressedLength + 1 + String.valueOf(origin.length()).length())))
				return origin;
			
			if (compressedLength > 20)
			{
				compressedLength = 20;
				logger.warn("compressString function compressedLength must less 20");
			}
			
			StringBuilder result = new StringBuilder();
			
			result.append(origin.length()).append(origin.charAt(origin.length()/2));
				
			try
			{
				byte[] dest = DigestUtils.md5(origin);
				long destLong1 = 0;
				long destLong2 = 0;
				
				for(int i = 0 ; i < 8; i++)
				{
					destLong1 |= dest[i] & 0xff;
					destLong1 <<= 8;
				}
				
				for(int i = 8 ; i < 15; i++)
				{
					destLong2 |= dest[i] & 0xff;
					destLong2 <<= 8;
				}
				
				if (compressedLength <= 10 )
				{
					for(int j = 0 ; j < compressedLength; j++)
					{
						long index = destLong1 & 0x0000003F;
						result.append(dict[(int)index]);
						destLong1 >>= 6;
					}
				}
				else
				{
					for(int j = 0 ; j < 10; j++)
					{
						long index = destLong1 & 0x0000003F;
						result.append(dict[(int)index]);
						destLong1 >>= 6;
					}
					
					for(int j = 0 ; j < compressedLength - 10; j++)
					{
						long index = destLong2 & 0x0000003F;
						result.append(dict[(int)index]);
						destLong2 >>= 6;
					}
				}
				
			}
			catch(Exception ex)
			{
				logger.error("comporessString error !",ex);
			}
			
			return result.toString();
		}
		
		public static void main(String[] args)
		{
			String test = "1234567890abcdefg";
			System.out.println(compressString(test,8));
			System.out.println(compressString(test,10));
			System.out.println(compressString(test,14));
		}

}
