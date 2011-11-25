/**
 * 
 */
package com.taobao.top.analysis.exception;

/**
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
