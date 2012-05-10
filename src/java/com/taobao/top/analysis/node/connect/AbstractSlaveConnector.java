/**
 * 
 */
package com.taobao.top.analysis.node.connect;

import com.taobao.top.analysis.config.SlaveConfig;
import com.taobao.top.analysis.node.component.SlaveNode;


/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-2 下午5:22:20
 *
 */
public abstract class AbstractSlaveConnector implements ISlaveConnector {

	SlaveConfig config;
	
	SlaveNode slaveNode; 
	
	@Override
	public SlaveConfig getConfig() {
		return config;
	}

	
	@Override
	public void setConfig(SlaveConfig config) {
		this.config = config;
	}


    /**
     * @return the slaveNode
     */
    public SlaveNode getSlaveNode() {
        return slaveNode;
    }


    /**
     * @param slaveNode the slaveNode to set
     */
    public void setSlaveNode(SlaveNode slaveNode) {
        this.slaveNode = slaveNode;
    }

}
