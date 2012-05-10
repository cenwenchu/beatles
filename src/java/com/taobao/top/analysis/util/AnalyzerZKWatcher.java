/**
 * 
 */
package com.taobao.top.analysis.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.taobao.top.analysis.config.IConfig;

/**
 * 简单的监控节点数据变化，将变化的数据设置到config中
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午1:31:34
 *
 */
public class AnalyzerZKWatcher<C extends IConfig> implements Watcher{
	
	private static final Log logger = LogFactory.getLog(AnalyzerZKWatcher.class);
	
	ZooKeeper zk;
	C config;
	
	public AnalyzerZKWatcher(C config)
	{
		this.config = config;
	}
	

	public ZooKeeper getZk() {
		return zk;
	}



	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}


	@Override
	public void process(WatchedEvent event) {
		switch(event.getType())
		{
			case None:
				
				break;
			
			case NodeCreated:
				
				break;
				
			case NodeDeleted:
				
				break;
				
			case NodeDataChanged:
				try
				{
					String path = event.getPath();
					byte[] result = zk.getData(path, true, null);
					String content = new String(result,"utf-8");
					
					if (logger.isWarnEnabled())
					{
						logger.warn(new StringBuilder("config : ")
							.append(path).append(" change, value : ").append(content).toString());
								
					}
					
					config.unmarshal(content);
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				
				break;
				
			case NodeChildrenChanged:
				
				break;
				
			default:
		
		}
	}

}
