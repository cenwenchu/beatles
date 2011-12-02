package com.taobao.top.analysis.node.connect;


import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.node.IComponent;
import com.taobao.top.analysis.node.component.MasterNode;
import com.taobao.top.analysis.node.event.GetTaskResponseEvent;
import com.taobao.top.analysis.node.event.SendResultsResponseEvent;

/**
 * Master端的通信组件，主要用于服务端与客户端通信，可自定义扩展为内存交互，socket交互，db交互等
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public interface IMasterConnector extends IComponent<MasterConfig>{
	
	public void setMasterNode(MasterNode masterNode);
	
	/**
	 * 响应获取任务数据的请求
	 * @param 请求事件
	 */
	public void echoGetJobTasks(GetTaskResponseEvent event);
	
	/**
	 * 响应发送任务结果的请求
	 * @param 返回结果
	 */
	public void echoSendJobTaskResults(SendResultsResponseEvent event);

}
