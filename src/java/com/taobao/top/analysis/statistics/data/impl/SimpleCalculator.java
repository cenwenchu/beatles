package com.taobao.top.analysis.statistics.data.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.ICalculator;
import com.taobao.top.analysis.statistics.data.ObjectColumn;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;

public class SimpleCalculator implements ICalculator {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8024981401877328739L;
	/**
	 * value表达式中的变量列表
	 */
	private List<Object> bindingStack;
	/**
	 * value表达式中的操作列表
	 */
	private List<Byte> operatorStack;
	
	private final String value;
	
	/**
	 * 关联使用的entry
	 */
	private Set<String> referEntries;
	
	/**
     * @return the referEntries
     */
    public Set<String> getReferEntries() {
        return referEntries;
    }

    public String getValue() {
		return value;
	}

	public List<Object> getBindingStack() {
		return bindingStack;
	}

	public List<Byte> getOperatorStack() {
		return operatorStack;
	}
	
	public void init(Map<String, Alias> aliasPool)  throws AnalysisException{

	    //从这里init的代码可以看出，分析器的配置解析规则，并非是全部读取后，然后进行一次性解析的，它的report配置是有固定顺序的
	    //一旦顺序出错，则会引起出错，同时alias和entry已经#的常量，三者是不可以重名的，否则会出错，这些潜规则都是在代码里反应出的
	    //后续可以对这些进行改造，毕竟这并非是一个友好而完善的解析方式

		if (value != null
				&& !"".equals(value)
				&& (value.indexOf("$") >= 0 || value
						.indexOf("entry(") >= 0)) {

			bindingStack = new ArrayList<Object>();
			operatorStack = new ArrayList<Byte>();
			referEntries = new HashSet<String>();

			String c = value;
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
							if (aliasPool != null && aliasPool.size() > 0
									&& temp.indexOf(".") > 0 
									&& aliasPool.get(temp.substring(0, temp.indexOf("."))) != null)
							{
								bindingStack.add(new ObjectColumn(aliasPool.get(temp.substring(0, temp.indexOf("."))).getKey(),
									temp.substring(temp.indexOf(".")+1)));
							}
							else
								bindingStack.add(Integer.valueOf(temp));

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
				referEntries.add(temp);
			}

			char[] cs = value.toCharArray();

			for (char _ch : cs) {
				if (_ch == '+' || _ch == '-' || _ch == '*' || _ch == '/')
					operatorStack.add(ReportUtil.generateOperationFlag(_ch));
			}

		}
	
	}


	public SimpleCalculator(String valueExpression,
			Map<String, Alias> aliasPool) throws AnalysisException {
		this.value = valueExpression;
		init(aliasPool);
	}

	@Override
	public Object calculator(Object[] contents) {


		Object result = null;

		double left = 0;

		if (bindingStack != null
				&& bindingStack.size() > 0) {
			if (bindingStack.size() > 1) {
				if (bindingStack.get(0) instanceof String && ((String)bindingStack.get(0)).startsWith("#"))
					left = Double.valueOf(((String)bindingStack.get(0)).substring(1));
				else 
					if (bindingStack.get(0) instanceof ObjectColumn)
					{
						if (((ObjectColumn)bindingStack.get(0)).getcIndex() - 1 >= contents.length)
							return result;
						Object o = contents[((ObjectColumn)bindingStack.get(0)).getcIndex() - 1];
						
						o = ReportUtil.getValueFromJosnObj(o.toString(),((ObjectColumn)bindingStack.get(0)).getSubKeyName());
						
						if(o instanceof Number){
							left = ((Number) o).doubleValue();
						}else{
							left = Double.valueOf(o.toString());
						}
					}
					else
					{
						if ((Integer)bindingStack.get(0) - 1 >= contents.length)
							return result;
						Object o = contents[(Integer)bindingStack.get(0) - 1];
						if(o instanceof Number){
							left = ((Number) o).doubleValue();
						}else{
							left = Double.valueOf(o.toString());
						}
						
					}

				double right = 0;

				int size = bindingStack.size();

				for (int i = 0; i < size - 1; i++) {
					if (bindingStack.get(i + 1) instanceof String && ((String)bindingStack.get(i + 1)).startsWith("#"))
						right = Double.valueOf(((String)bindingStack.get(i + 1))
								.substring(1));
					else 
						if (bindingStack.get(i + 1) instanceof ObjectColumn)
						{
							
							if (((ObjectColumn)bindingStack.get(i+1)).getcIndex() - 1 >= contents.length)
								return result;
							
							Object o = contents[((ObjectColumn)bindingStack.get(i+1)).getcIndex() - 1];
							
							o = ReportUtil.getValueFromJosnObj(o.toString(),((ObjectColumn)bindingStack.get(i+1)).getSubKeyName());
							
							if(o instanceof Number){
								right = ((Number) o).doubleValue();
							}else{
								right = Double.valueOf(o.toString());
							}
						}
						else
						{
							if ((Integer)bindingStack.get(i + 1) - 1 >= contents.length)
								return result;
							Object o = contents[(Integer)bindingStack.get(i + 1) - 1];
							if(o instanceof Number){
								right = ((Number) o).doubleValue();
							}else{
								right = Double.valueOf(o.toString());
							}
						}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_PLUS)
					{
						left += right;
						continue;
					}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_MINUS)
					{
						left -= right;
						continue;
					}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_RIDE)
					{
						left = left * right;
						continue;
					}

					if (operatorStack.get(i) == AnalysisConstants.OPERATE_DIVIDE)
					{
						left = left / right;
						continue;
					}

				}

				result = left;
			} else {
				if (bindingStack.get(0) instanceof String && ((String)bindingStack.get(0)).startsWith("#"))
					result = Double.valueOf(((String)bindingStack.get(0)).substring(1));
				else 
					if (bindingStack.get(0) instanceof ObjectColumn)
					{
						if (((ObjectColumn)bindingStack.get(0)).getcIndex() - 1 >= contents.length)
							return result;
	
						result = contents[((ObjectColumn)bindingStack.get(0)).getcIndex() - 1];
											
						result = ReportUtil.getValueFromJosnObj(result.toString(),((ObjectColumn)bindingStack.get(0)).getSubKeyName());
					}
					else
					{
						if ((Integer)bindingStack.get(0) - 1 >= contents.length)
							return result;
	
						result = contents[(Integer)bindingStack.get(0) - 1];
					}

			}

		}

		return result;
	}

}
