package com.taobao.top.analysis.util;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.top.analysis.node.event.MasterNodeEvent;

public class TimeOutQueueTest {
	Map<String,MasterNodeEvent> responseQueue = new ConcurrentHashMap<String,MasterNodeEvent>();
	TimeOutQueueExt timeOutQueue = new TimeOutQueueExt();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws InterruptedException {
		
		MasterNodeEvent eventMock = new MasterNodeEvent();
		eventMock.setEventCreateTime(System.currentTimeMillis());
		eventMock.setMaxEventHoldTime(2);
		eventMock.setSequence("1");
		
		MasterNodeEvent eventMock2 = new MasterNodeEvent();
		eventMock2.setEventCreateTime(System.currentTimeMillis());
		eventMock2.setMaxEventHoldTime(5);
		eventMock2.setSequence("2");
		
		MasterNodeEvent eventMock3 = new MasterNodeEvent();
		eventMock3.setEventCreateTime(System.currentTimeMillis());
		eventMock3.setMaxEventHoldTime(0);
		eventMock3.setSequence("3");
		
		MasterNodeEvent eventMock4 = new MasterNodeEvent();
		eventMock4.setEventCreateTime(System.currentTimeMillis());
		eventMock4.setMaxEventHoldTime(10);
		eventMock4.setSequence("4");
		
		timeOutQueue.add(eventMock);
		timeOutQueue.add(eventMock2);
		timeOutQueue.add(eventMock3);
		timeOutQueue.add(eventMock4);
		
		Assert.assertEquals(timeOutQueue.size(), 4);
		
		Thread.sleep(2002);
		
		Assert.assertEquals(timeOutQueue.size(), 3);
		
		Thread.sleep(3000);
		
		Assert.assertEquals(timeOutQueue.size(), 2);
		
		MasterNodeEvent eventMockTmp = timeOutQueue.poll();
		
		Assert.assertEquals(eventMockTmp.getSequence(), "4");
		
		eventMockTmp = timeOutQueue.poll();
		
		Assert.assertEquals(eventMockTmp.getSequence(), "3");
		
	}
	
	
	
	class TimeOutQueueExt extends TimeOutQueue<MasterNodeEvent>
	{

		@Override
		public void timeOutAction(MasterNodeEvent event) {
			if(responseQueue.containsKey(event.getSequence()))
			{
				responseQueue.remove(event.getSequence());
				
				System.out.println(System.currentTimeMillis());
				System.out.println(event.getEventCreateTime());
			}
		}
		
	}

}
