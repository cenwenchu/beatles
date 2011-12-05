/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.node.component.MasterNode;

/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-2 下午5:17:39
 *
 */
public abstract class AbstractMasterConnector implements IMasterConnector {

	MasterNode masterNode;
	MasterConfig config;

	
	@Override
	public MasterConfig getConfig() {
		// TODO Auto-generated method stub
		return config;
	}

	
	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}
	
	@Override
	public void setMasterNode(MasterNode masterNode) {
		this.masterNode = masterNode;
	}


}
