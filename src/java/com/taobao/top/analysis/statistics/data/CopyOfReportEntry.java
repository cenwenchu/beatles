/**
 * 
 */
package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.statistics.map.IMapper;
import com.taobao.top.analysis.statistics.reduce.IReducer;
import com.taobao.top.analysis.statistics.reduce.group.GroupFunctionFactory;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;

/**
 * SimpleMapReduce Entry
 * 
 * @author fangweng
 * 
 */
public class CopyOfReportEntry implements Serializable, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6275004376213449537L;

	private String id;
	/**
	 * 统计的主键列描述
	 */
	private int[] keys;
	/**
	 * 报表Entry统计模式，支持最小，最大，总和，计数，平均，直接显示
	 */
	private GroupFunctionFactory valueType;
	/**
	 * 具体的表达式
	 */
	private String valueExpression;
	/**
	 * 显示在报表的列title
	 */
	private String name;
	/**
	 * 父ReportEntry的ID,可以为空
	 */
	private String parent;

	/**
	 * 自定义key生成方法
	 */
	private IMapper mapClass;
	/**
	 * 自定义value生成方法
	 */
	private IReducer reduceClass;
	/**
	 * mapClass带入的参数
	 */
	private String mapParams;
	/**
	 * reduceClass带入的参数
	 */
	private String reduceParams;
	/**
	 * value表达式中的变量列表
	 */
	private List<Object> bindingStack;
	/**
	 * value表达式中的操作列表
	 */
	private List<Byte> operatorStack;
	/**
	 * 是否采用js引擎来解析，效率比较低，但是功能强大
	 */
	private String engine;
	/**
	 * 是否是后处理的列，例如有些列需要由某几个统计列再次作处理， 因此必须在这些列处理以后再统计
	 */
	private boolean lazy;
	/**
	 * key的条件设置
	 */
	private String conditions;
	/**
	 * value的过滤设置
	 */
	private String valuefilter;
	/**
	 * condition的key保留队列
	 */
	private List<Object> conditionKStack;
	/**
	 * condition的value保留队列
	 */
	private List<Object> conditionVStack;
	/**
	 * condition的操作保留队列
	 */
	private List<Byte> conditionOpStack;

	/**
	 * 条件之间是否是与的关系，当前只支持全部是与或者全部是或。
	 */
	private boolean andCondition = true;
	/**
	 * 值过滤条件
	 */
	private List<Object> valuefilterStack;
	/**
	 * 值过滤条件中的操作符列表
	 */
	private List<Byte> valuefilterOpStack;
	/**
	 * 是否需要格式化结果，在报表生成的时候格式化，当前支持round
	 */
	private List<String> formatStack;

	/**
	 * 全局的mapclass
	 */
	private List<String> globalMapClass;
	
	/**
	 * 可以为单独报表定制是否需要压缩
	 */
	private String useCompressKeyMode;
	
	//mod by fangweng 2011 performance
	//配合useCompressKeyMode来设置压缩字符的长度，越小越容易冲突,最大20
	private int compressedDestMaxLength = 14;
	

	public String getUseCompressKeyMode() {
		return useCompressKeyMode;
	}

	public void setUseCompressKeyMode(String useCompressKeyMode) {
		this.useCompressKeyMode = useCompressKeyMode;
	}

	public int getCompressedDestMaxLength() {
		return compressedDestMaxLength;
	}

	public void setCompressedDestMaxLength(int compressedDestMaxLength) {
		this.compressedDestMaxLength = compressedDestMaxLength;
	}

	public boolean isAndCondition() {
		return andCondition;
	}

	public List<String> getGlobalMapClass() {
		return globalMapClass;
	}

	public void setGlobalMapClass(List<String> globalMapClass) {
		this.globalMapClass = globalMapClass;
	}

	public List<Byte> getValuefilterOpStack() {
		return valuefilterOpStack;
	}

	public void setValuefilterOpStack(List<Byte> valuefilterOpStack) {
		this.valuefilterOpStack = valuefilterOpStack;
	}

	public List<Byte> getConditionOpStack() {
		return conditionOpStack;
	}

	public void setConditionOpStack(List<Byte> conditionOpStack) {
		this.conditionOpStack = conditionOpStack;
	}

	public List<String> getFormatStack() {
		return formatStack;
	}

	public void setFormatStack(List<String> formatStack) {
		this.formatStack = formatStack;
	}

	public String getValuefilter() {
		return valuefilter;
	}

	public void setValuefilter(String valuefilter) throws AnalysisException {
		this.valuefilter = valuefilter;

		if (valuefilter != null && !"".equals(valuefilter)) {
			String[] filters = StringUtils.split(valuefilter, "&");

			valuefilterStack = new ArrayList<Object>();
			valuefilterOpStack = new ArrayList<Byte>();

			formatStack = new ArrayList<String>();

			for (String f : filters) {
				if (f.startsWith(AnalysisConstants.CONDITION_ROUND_STR))
					formatStack.add(f);
				else {
					if (f.startsWith(AnalysisConstants.CONDITION_ISNUMBER_STR)) {
						valuefilterOpStack.add(ReportUtil.generateOperationFlag(f));
						valuefilterStack.add(f);
					} else {

						if (f.startsWith(AnalysisConstants.CONDITION_EQUALORGREATER_STR)
								|| f.startsWith(AnalysisConstants.CONDITION_EQUALORLESSER_STR)
								|| f.startsWith(AnalysisConstants.CONDITION_NOT_EQUAL_STR)) {
							valuefilterOpStack.add(ReportUtil.generateOperationFlag(f.substring(0, 2)));
							
							if (f.startsWith(AnalysisConstants.CONDITION_NOT_EQUAL_STR))
								valuefilterStack.add(f.substring(2));
							else
								valuefilterStack.add(Double.valueOf(f.substring(2)));
							
						} else {
							if (f.startsWith(AnalysisConstants.CONDITION_EQUAL_STR)
									|| f.startsWith(AnalysisConstants.CONDITION_GREATER_STR)
									|| f.startsWith(AnalysisConstants.CONDITION_LESSER_STR)) {
								valuefilterOpStack.add(ReportUtil.generateOperationFlag(f.substring(0, 1)));
								
								if (f.startsWith(AnalysisConstants.CONDITION_EQUAL_STR))
									valuefilterStack.add(f.substring(1));
								else
									valuefilterStack.add(Double.valueOf(f.substring(1)));
							}
						}
					}
				}
			}

		}
	}

	public List<Object> getValuefilterStack() {
		return valuefilterStack;
	}

	public void setValuefilterStack(List<Object> valuefilterStack) {
		this.valuefilterStack = valuefilterStack;
	}

	public List<Object> getConditionKStack() {
		return conditionKStack;
	}

	public void setConditionKStack(List<Object> conditionKStack) {
		this.conditionKStack = conditionKStack;
	}

	public List<Object> getConditionVStack() {
		return conditionVStack;
	}

	public void setConditionVStack(List<Object> conditionVStack) {
		this.conditionVStack = conditionVStack;
	}

	public String getConditions() {
		return conditions;
	}

	public void setConditions(String conditions, Map<String, Alias> aliasPool) throws AnalysisException {
		this.conditions = conditions;

		if (conditions != null && !"".equals(conditions)) {
			conditionKStack = new ArrayList<Object>();
			conditionVStack = new ArrayList<Object>();
			conditionOpStack = new ArrayList<Byte>();

			String[] cons;

			if (conditions.indexOf("||") > 0) {
				cons = StringUtils.split(conditions, "||");
				this.andCondition = false;
			} else {
				cons = StringUtils.split(conditions, "&");
				this.andCondition = true;
			}

			for (String con : cons) {
				if (!con.startsWith("$"))
					continue;

				Object key = ReportUtil.transformVar(
						con.substring(1, con.lastIndexOf("$")), aliasPool);
				String value = con.substring(con.lastIndexOf("$") + 1);
				String operate = null;

				if (value.startsWith(AnalysisConstants.CONDITION_NOT_EQUAL_STR)
						|| value.startsWith(AnalysisConstants.CONDITION_EQUALORGREATER_STR)
						|| value.startsWith(AnalysisConstants.CONDITION_EQUALORLESSER_STR)) {
					operate = value.substring(0, 2);
					value = value.substring(2);
				} else {
					if (value.startsWith(AnalysisConstants.CONDITION_EQUAL_STR)
							|| value.startsWith(AnalysisConstants.CONDITION_LESSER_STR)
							|| value.startsWith(AnalysisConstants.CONDITION_GREATER_STR)) {
						operate = value.substring(0, 1);
						value = value.substring(1);
					}
				}

				if (!key.equals(AnalysisConstants.RECORD_LENGTH))
				{
					conditionKStack.add(key);
					
					if (operate.equals(AnalysisConstants.CONDITION_EQUAL_STR)
							|| operate.equals(AnalysisConstants.CONDITION_NOT_EQUAL_STR))
					{
						conditionVStack.add(value);
					}
					else
						conditionVStack.add(Double.valueOf(value));
				}
				else
				{
					conditionKStack.add(key);
					conditionVStack.add(Integer.valueOf(value));
				}

				
				conditionOpStack.add(ReportUtil.generateOperationFlag(operate));
			}
		}

	}

	public void appendConditions(String conditions, Map<String, Alias> aliasPool) throws AnalysisException {
		if (this.conditions != null && !"".equals(this.conditions)) {
			this.conditions += "&" + conditions;
		} else {
			this.conditions = conditions;
		}

		this.setConditions(this.conditions, aliasPool);

	}

	public boolean isLazy() {
		return lazy;
	}

	public void setLazy(boolean lazy) {
		this.lazy = lazy;
	}

	public String getEngine() {
		return engine;
	}

	public void setEngine(String engine) {
		this.engine = engine;
	}

	public String getMapParams() {
		return mapParams;
	}

	public void setMapParams(String mapParams) {
		this.mapParams = mapParams;
	}

	public String getReduceParams() {
		return reduceParams;
	}

	public void setReduceParams(String reduceParams) {
		this.reduceParams = reduceParams;
	}

	public IMapper getMapClass() {
		return mapClass;
	}

	public void setMapClass(IMapper mapClass) {
		this.mapClass = mapClass;
	}

	public IReducer getReduceClass() {
		return reduceClass;
	}

	public void setReduceClass(IReducer reduceClass) {
		this.reduceClass = reduceClass;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int[] getKeys() {
		return keys;
	}

	public void setKeys(int[] keys) {
		this.keys = keys;
	}

	public GroupFunctionFactory getValueType() {
		return valueType;
	}

	public void setValueType(GroupFunctionFactory valueType) {
		this.valueType = valueType;
	}

	public String getValueExpression() {
		return valueExpression;
	}

	public void setValueExpression(String valueExpression,
			Map<String, Alias> aliasPool) throws AnalysisException {
		this.valueExpression = valueExpression;

		if (valueExpression != null
				&& !"".equals(valueExpression)
				&& (valueExpression.indexOf("$") >= 0 || valueExpression
						.indexOf("entry(") >= 0)) {
			if (valueExpression.indexOf("entry(") >= 0)
				this.setLazy(true);

			bindingStack = new ArrayList<Object>();
			operatorStack = new ArrayList<Byte>();

			String c = valueExpression;
			String temp;

			while (c.indexOf("$") >= 0 || c.indexOf("#") >= 0) {
				if (c.indexOf("$") >= 0) {
					if (c.indexOf("#") < 0
							|| (c.indexOf("#") >= 0 && c.indexOf("$") < c
									.indexOf("#"))) {
						c = c.substring(c.indexOf("$") + 1);
						temp = c.substring(0, c.indexOf("$"));
						c = c.substring(c.indexOf("$") + 1);

						if (aliasPool != null && aliasPool.size() > 0
								&& aliasPool.get(temp) != null) {
							bindingStack.add(aliasPool.get(temp).getKey());
						} else
							bindingStack.add(temp);

						continue;
					}
				}

				if (c.indexOf("#") >= 0) {
					if (c.indexOf("$") < 0
							|| (c.indexOf("$") >= 0 && c.indexOf("$") > c
									.indexOf("#"))) {
						c = c.substring(c.indexOf("#") + 1);
						temp = c.substring(0, c.indexOf("#"));
						c = c.substring(c.indexOf("#") + 1);

						bindingStack.add("#" + temp);

						continue;
					}
				}

			}

			while (c.indexOf("entry(") >= 0) {
				c = c.substring(c.indexOf("entry(") + "entry(".length());
				temp = c.substring(0, c.indexOf(")"));
				c = c.substring(c.indexOf(")") + 1);
				bindingStack.add(temp);
			}

			char[] cs = valueExpression.toCharArray();

			for (char _ch : cs) {
				if (_ch == '+' || _ch == '-' || _ch == '*' || _ch == '/')
					operatorStack.add(ReportUtil.generateOperationFlag(_ch));
			}

		}
	}

	public List<Object> getBindingStack() {
		return bindingStack;
	}

	public void setBindingStack(List<Object> bindingStack) {
		this.bindingStack = bindingStack;
	}

	public List<Byte> getOperatorStack() {
		return operatorStack;
	}

	public void setOperatorStack(List<Byte> operatorStack) {
		this.operatorStack = operatorStack;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	@Override
	public CopyOfReportEntry clone() {
		CopyOfReportEntry o = null;
		try {
			o = (CopyOfReportEntry) super.clone();

			if (conditionKStack != null) {
				o.conditionKStack = new ArrayList<Object>();
				for (Object obj : conditionKStack) {
					o.conditionKStack.add(obj);
				}
			}
			if (conditionVStack != null) {
				o.conditionVStack = new ArrayList<Object>();
				for (Object str : conditionVStack) {
					o.conditionVStack.add(str);
				}
			}
			if (conditionOpStack != null) {
				o.conditionOpStack = new ArrayList<Byte>();
				for (Byte str : conditionOpStack) {
					o.conditionOpStack.add(str);
				}
			}
			if (keys != null) {
				o.keys = new int[this.keys.length];
				System.arraycopy(this.keys, 0, o.keys, 0, keys.length);
			}
			if (valueType != null) {
				o.valueType = this.valueType;
			}
			if (bindingStack != null) {
				o.bindingStack = new ArrayList<Object>();
				for (Object str : bindingStack) {
					o.bindingStack.add(str);
				}
			}
			if (operatorStack != null) {
				o.operatorStack = new ArrayList<Byte>();
				for (Byte str : operatorStack) {
					o.operatorStack.add(str);
				}
			}
			if (valuefilterStack != null) {
				o.valuefilterStack = new ArrayList<Object>();
				for (Object str : valuefilterStack) {
					o.valuefilterStack.add(str);
				}
			}
			if (valuefilterOpStack != null) {
				o.valuefilterOpStack = new ArrayList<Byte>();
				for (Byte str : valuefilterOpStack) {
					o.valuefilterOpStack.add(str);
				}
			}
			if (formatStack != null) {
				o.formatStack = new ArrayList<String>();
				for (String str : formatStack) {
					o.formatStack.add(str);
				}
			}
			if (globalMapClass != null) {
				o.globalMapClass = new ArrayList<String>();
				for (String str : globalMapClass) {
					o.globalMapClass.add(str);
				}
			}
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return o;
	}

}
