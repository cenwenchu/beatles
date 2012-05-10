/**
 * 
 */
package com.taobao.top.analysis.log;
import org.apache.log4j.DailyRollingFileAppender;

import com.taobao.top.analysis.config.AbstractConfig;

/**
 * 
 */
public class TopDailyRollingFileAppender extends DailyRollingFileAppender {
	@Override
	public void activateOptions() {
		this.fileName+= "." + AbstractConfig.getSystemName();
		super.activateOptions();
	}
}
