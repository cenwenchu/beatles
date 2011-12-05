/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import com.taobao.top.analysis.config.SlaveConfig;


/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-2 下午5:22:20
 *
 */
public abstract class AbstractSlaveConnector implements ISlaveConnector {

	SlaveConfig config;
	
	@Override
	public SlaveConfig getConfig() {
		return config;
	}

	
	@Override
	public void setConfig(SlaveConfig config) {
		this.config = config;
	}

}
