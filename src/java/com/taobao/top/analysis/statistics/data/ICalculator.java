package com.taobao.top.analysis.statistics.data;

import java.io.Serializable;

public interface ICalculator extends Cloneable,Serializable{
	
	public Object calculator(Object[] content);

}
