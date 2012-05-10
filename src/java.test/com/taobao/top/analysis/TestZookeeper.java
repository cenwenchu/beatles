/**
 * 
 */
package com.taobao.top.analysis;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.taobao.top.analysis.util.AnalysisConstants;

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
		
		String servers = "127.0.0.1:2181";
		String groupId = "group1";
		String master1 = "master1";
		String master2 = "master2";
		String slave1 = "slave1";
		String slave2 = "slave2";
		String slave3 = "slave3";
		String config1 = "config1";
		
		TestZookeeper tz = new TestZookeeper();
		ZooKeeper zk = new ZooKeeper(servers,3000,tz);
		tz.zk = zk;
		
		//每次启动时都先检查是否有根目录
		createGroupNodesIfNotExist(zk,groupId);
		
		updateOrCreateNode(zk,getGroupMasterZKPath(groupId)+"/"+master1,"master1value".getBytes("UTF-8"));
		updateOrCreateNode(zk,getGroupMasterZKPath(groupId)+"/"+master2,"master2value".getBytes("UTF-8"));
		updateOrCreateNode(zk,getGroupSlaveZKPath(groupId)+"/"+slave1,"slave1value".getBytes("UTF-8"));
		updateOrCreateNode(zk,getGroupSlaveZKPath(groupId)+"/"+slave2,"slave2value".getBytes("UTF-8"));
		updateOrCreateNode(zk,getGroupSlaveZKPath(groupId)+"/"+slave3,"slave3value".getBytes("UTF-8"));
		updateOrCreateNode(zk,getGroupConfigZKPath(groupId)+"/"+config1,"config1value".getBytes("UTF-8"));
		
		List<String> groups = zk.getChildren(AnalysisConstants.ZK_ROOT, false);
		
		List<String> master = zk.getChildren(getGroupMasterZKPath(groups.get(0)), false);
				
		List<String> slave = zk.getChildren(getGroupSlaveZKPath(groups.get(0)), false);
		
		List<String> config = zk.getChildren(getGroupConfigZKPath(groups.get(0)), false);
		
		System.out.println(new String(zk.getData(getGroupConfigZKPath(groups.get(0)) + "/" + config.get(0), false,null),"utf-8"));
		
		Thread.sleep(200000);

	}
	
	protected static void createGroupNodesIfNotExist(ZooKeeper zk,String groupId) throws KeeperException, InterruptedException
	{
		if (zk.exists(getGroupZKPath(groupId), false) == null)
		{
			createNodeIfNotExist(zk,AnalysisConstants.ZK_ROOT,new byte[0]);
			createNodeIfNotExist(zk,getGroupZKPath(groupId), new byte[0]);
			createNodeIfNotExist(zk,getGroupMasterZKPath(groupId), new byte[0]);
			createNodeIfNotExist(zk,getGroupSlaveZKPath(groupId), new byte[0]);
			createNodeIfNotExist(zk,getGroupConfigZKPath(groupId), new byte[0]);
		}
	}
	
	protected static String getGroupZKPath(String groupId)
	{
		return new StringBuilder().append(AnalysisConstants.ZK_ROOT).append("/")
				.append(groupId).toString();
	}
	
	protected static String getGroupMasterZKPath(String groupId)
	{
		return new StringBuilder().append(getGroupZKPath(groupId)).append(AnalysisConstants.ZK_MASTER).toString();
	}
	
	protected static String getGroupSlaveZKPath(String groupId)
	{
		return new StringBuilder().append(getGroupZKPath(groupId)).append(AnalysisConstants.ZK_SLAVE).toString();
	}
	
	protected static String getGroupConfigZKPath(String groupId)
	{
		return new StringBuilder().append(getGroupZKPath(groupId)).append(AnalysisConstants.ZK_CONFIG).toString();
	}
	
	
	protected static void createNodeIfNotExist(ZooKeeper zk,String path,byte[] data) throws KeeperException, InterruptedException
	{
		Stat node = zk.exists(path, false);
		
		if (node == null)
		{ 
			zk.create(path,data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	protected static void updateOrCreateNode(ZooKeeper zk,String path,byte[] data) throws KeeperException, InterruptedException
	{
		Stat node = zk.exists(path, false);
		
		if (node == null)
		{
			zk.create(path,data,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else
		{
			//不管什么情况都覆盖已有的数据
			zk.setData(path, data, -1);
		}
		
		
		//增加对于节点数据修改的监控
		zk.getData(path, true, null);
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
