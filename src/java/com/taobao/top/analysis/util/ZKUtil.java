/**
 * 
 */
package com.taobao.top.analysis.util;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * zookeeper的一个工具类，当前的目录结构：
 *  
 *  beatles-
 *  		groupId-
 *  				master-
 *  					runtime-
 *  						epoch-
 *  						jobName-
 *  							taskId-
 *  				slave-
 *  				config-
 *  
 *  
 * @author fangweng
 * email: fangweng@taobao.com
 * 下午1:22:11
 *
 */
public class ZKUtil {

	
	/**
	 * 创建某一个Group的配直节点
	 * @param zk
	 * @param groupId
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void createGroupNodesIfNotExist(ZooKeeper zk,String groupId) throws KeeperException, InterruptedException
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
	
	/**
	 * 获得某一个group的zk节点
	 * @param groupId
	 * @return
	 */
	public static String getGroupZKPath(String groupId)
	{
		return new StringBuilder().append(AnalysisConstants.ZK_ROOT).append("/")
				.append(groupId).toString();
	}
	
	/**
	 * 获得某一个group的master节点
	 * @param groupId
	 * @return
	 */
	public static String getGroupMasterZKPath(String groupId)
	{
		return new StringBuilder().append(getGroupZKPath(groupId)).append(AnalysisConstants.ZK_MASTER).toString();
	}
	
	/**
	 * 获得某一个group的slave节点
	 * @param groupId
	 * @return
	 */
	public static String getGroupSlaveZKPath(String groupId)
	{
		return new StringBuilder().append(getGroupZKPath(groupId)).append(AnalysisConstants.ZK_SLAVE).toString();
	}
	
	/**
	 * 获得某一个group的config节点
	 * @param groupId
	 * @return
	 */
	public static String getGroupConfigZKPath(String groupId)
	{
		return new StringBuilder().append(getGroupZKPath(groupId)).append(AnalysisConstants.ZK_CONFIG).toString();
	}
	

	/**
	 * 节点基础操作，根据路径判断是否存在该节点，不存在就创建，存在则不处理
	 * @param zk
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void createNodeIfNotExist(ZooKeeper zk,String path,byte[] data) throws KeeperException, InterruptedException
	{
		Stat node = zk.exists(path, false);
		
		if (node == null)
		{ 
			zk.create(path,data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	/**
	 * 节点基础操作，根据路径判断节点是否存在，如果存在则更新数据，如果不存在则创建节点
	 * @param zk
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void updateOrCreateNode(ZooKeeper zk,String path,byte[] data) throws KeeperException, InterruptedException
	{
		Stat node = zk.exists(path, false);
		
		if (node == null)
		{
			createPath(zk,path);
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
	
	/**
	 * 循环创建路径中不存在的节点
	 * @param zk
	 * @param path
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public static void createPath(ZooKeeper zk,String path) throws KeeperException, InterruptedException
	{
		String[] paths = StringUtils.split(path, '/');
		StringBuilder sp = new StringBuilder();
		
		for(int i = 0 ; i < paths.length - 1; i++)
		{
			sp.append("/").append(paths[i]);
			
			Stat node = zk.exists(sp.toString(), false);
			
			if (node == null)
			{
				zk.create(sp.toString(),null,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
		
	}
	
	/**
	 * 删除zk节点
	 * @param zk
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void deleteNode(ZooKeeper zk,String path)throws KeeperException, InterruptedException
	{
		Stat node = zk.exists(path, false);
		
		if (node != null)
		{
			List<String> subPaths = null;
			
			try
			{
				subPaths = zk.getChildren(path, false);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			
			if (subPaths == null || subPaths.size() == 0)
				zk.delete(path, -1);
			else
			{
				for(String s : subPaths)
				{
					deleteNode(zk,new StringBuilder().append(path).append("/").append(s).toString());
				}
				
				zk.delete(path, -1);
			}
		}
	}
	
}
