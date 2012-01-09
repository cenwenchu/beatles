/**
 * 
 */
package com.taobao.top.analysis;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author fangweng
 * @email: fangweng@taobao.com
 * 想要实现两件事情，关注某一个节点的数据，修改某一个节点的数据
 * 2011-12-31 下午2:53:18
 *
 */
public class TestZookeeper implements Watcher{
	
	ZooKeeper zk;

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		
		String servers = "127.0.0.1:2088,127.0.0.1:2089";
		
		ZooKeeper zk = new ZooKeeper(servers,3000,new TestZookeeper());
		TestZookeeper tz = new TestZookeeper();
		tz.zk = zk;
		
		zk.setData("/beatles", "hello".getBytes("UTF-8"), -1);
		
		byte[] result = zk.getData("/beatles",tz, null);
		
		System.out.println(new String(result,"UTF-8"));
		
		Thread.sleep(200000);

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
					byte[] result = zk.getData(path, this, null);
					
					System.out.println("path: " + path + " ,value : " + new String(result,"UTF-8"));
					
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
