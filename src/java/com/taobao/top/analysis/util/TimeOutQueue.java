/**
 * 
 */
package com.taobao.top.analysis.util;


import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * 放入事件用于检测是否超时,先做成比较简化版的实现，后续可以扩展
 * @author fangweng
 * @email: fangweng@taobao.com
 * 2011-12-5 下午1:37:00
 *
 */
public abstract class TimeOutQueue<E extends TimeOutEvent>{
	
	private static final Log logger = LogFactory.getLog(TimeOutQueue.class);
	
	private PriorityBlockingQueue<E> taskEventPool;//等待状态变更的消息池
	private TimeOutChecker timeOutChecker;//后台检查消息变更资源池的线程
	private Semaphore poolIsEmpty = new Semaphore(1);//防止空循环检查消息变更资源池
	
	//这三个用于超时事件队列检查
	private ReentrantLock lock;
	private Condition checkCondition;
	private AtomicLong minTimeOutStamp;
	
	public TimeOutQueue()
	{
		lock = new ReentrantLock();
		checkCondition = lock.newCondition();
		minTimeOutStamp = new AtomicLong(0);
		
		taskEventPool = new PriorityBlockingQueue<E>(100);
		timeOutChecker = new TimeOutChecker();
		timeOutChecker.setDaemon(true);
		timeOutChecker.start();
	}
	
	public void release()
	{
		clean();
		
		if (timeOutChecker != null)
			timeOutChecker.stopThread();
	}
	
	public void clean()
	{
		if (taskEventPool != null)
			taskEventPool.clear();
		
		minTimeOutStamp.set(0);
	}
	
	/**
	 * 用于后台检查状态变更消息池,当前只负责Timeout状态检查
	 * @author fangweng
	 * @email fangweng@taobao.com
	 * @date 2011-5-17
	 *
	 */
	class TimeOutChecker extends Thread
	{
		
		public TimeOutChecker()
		{
			super("TimeOutChecker-thread");
		}
		
		boolean isRunning = true;
		
		public void run()
		{
			try
			{
				while(isRunning)
				{
					//checker没有竞争，所以这里还是可靠的，用于防止内部没有任何数据的空转
					poolIsEmpty.acquire();
					
					if (taskEventPool.isEmpty())
						continue;
					
					E node;
					long restTime = 0;
					
					while((node = taskEventPool.peek()) != null)
					{
						restTime = node.getEventCreateTime() + node.getMaxEventHoldTime() * 1000 - System.currentTimeMillis();
						
						if (node.getEventCreateTime() != 0 && restTime <= 0)
						{
							taskEventPool.poll();
							timeOutAction(node);
						}
						else
						{
							if (node.getEventCreateTime() == 0)
								restTime = 5 * 60 * 1000;//如果剩下的都没有超时事件了，则给5分钟
							
							break;
						}
					}
					
					//cpu time interval ,预估一下一个最小超时到来的情况，防止多次循环
					if (restTime > 10)
					{
						
						if (restTime > 5 * 60 * 1000)
						{
							if (logger.isInfoEnabled())
								logger.info("restTime : " + restTime + " so large.");
							
							restTime = 5 * 60 * 1000;
						}
						
						boolean flag = lock.tryLock();
						
						try
						{
							if (flag)
								checkCondition.await(restTime, TimeUnit.MILLISECONDS);
						}
						catch(InterruptedException ie)
						{
							//do nothing
						}
						catch(Exception ex)
						{
							logger.error(ex,ex);
						}
						finally
						{
							if (flag)
								lock.unlock();
						}
					}
					
					poolIsEmpty.release();
				}
			}
			catch (InterruptedException e) 
			{
				//do nothing
			}
			catch(Exception ex)
			{
				logger.error("TaskChecker end...",ex);
			}
		}
		
		public void stopThread()
		{
			isRunning = false;
			interrupt();
		}
	}

	/**
	 * 当有新的事件加入状态变更等待队列，
	 * 判断最小的timeout是否发生改变，选择性唤醒checker
	 * @param node
	 */
	public void eventChainChange(E node)
	{
		if (node.getEventCreateTime() > 0 && (minTimeOutStamp.get() == 0 || node.getEventCreateTime() < minTimeOutStamp.get()))
		{
			//不做并发控制
			minTimeOutStamp.set(node.getEventCreateTime());
			
			boolean flag = lock.tryLock();
		
			try
			{
				if (flag)
				{
					checkCondition.signalAll();
				}
			}
			catch(Exception ex)
			{
				logger.error(ex,ex);
			}
			finally
			{
				if (flag)
					lock.unlock();
			}
			
		}
	}

	public void clear() {
		taskEventPool.clear();
		
		if (poolIsEmpty.availablePermits() == 0)
			poolIsEmpty.release();
	}


	public boolean contains(Object o) {
		return taskEventPool.contains(o);
	}


	public boolean isEmpty() {
		return taskEventPool.isEmpty();
	}


	public boolean remove(Object o) {
		return taskEventPool.remove(o);
	}


	public int size() {
		return taskEventPool.size();
	}


	public boolean add(E e) {
		
		boolean result = taskEventPool.add(e);
		
		if (result)
		{
			eventChainChange(e);
		
			if (poolIsEmpty.availablePermits() == 0)
				poolIsEmpty.release();
		}
		
		return result;
	}


	public boolean offer(E e) {
		
		boolean result = taskEventPool.offer(e);
		
		if (result)
		{
			eventChainChange(e);
		
			if (poolIsEmpty.availablePermits() == 0)
				poolIsEmpty.release();
		}
		
		return result;
	}


	public E peek() {
		return taskEventPool.peek();
	}


	public E poll() {
		return taskEventPool.poll();
	}
	
	//需要对timeout做一些反应
	public abstract void timeOutAction(E event);


}
