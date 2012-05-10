/**
 * 
 */
package com.taobao.top.analysis.exception;

/**
 * 分析包中的异常基类
 * @author fangweng
 *
 */
public class AnalysisException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3513677313370574479L;
	
	public AnalysisException() {
		super();
	}

	public AnalysisException(String message, Throwable cause) {
		super(message, cause);
	}

	public AnalysisException(String message) {
		super(message);
	}

	public AnalysisException(Throwable cause) {
		super(cause);
	}

}
