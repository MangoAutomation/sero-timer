/**
 * Copyright (C) 2016 Infinite Automation Software. All rights reserved.
 * @author Terry Packer
 */
package com.serotonin.timer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Real time timer that ensures all tasks of the same type are 
 * run in order of the time they were submitted.
 * 
 * @author Terry Packer
 *
 */
public class OrderedRealTimeTimer extends RealTimeTimer implements RejectedExecutionHandler{
	
	public void init(OrderedThreadPoolExecutor executorService, int threadPriority){
		OrderedTimerThread timer = new OrderedTimerThread(queue, executorService, timeSource);
        timer.setName("Ordered RealTime Timer");
        timer.setDaemon(false);
        timer.setPriority(threadPriority);
        super.init(timer);
	}
	
	@Override
	public void init(ExecutorService executor){
		OrderedThreadPoolExecutor otpExecutor = (OrderedThreadPoolExecutor) executor;
		otpExecutor.setRejectedExecutionHandler(this);
		this.init(otpExecutor, Thread.MAX_PRIORITY);
	}
	
	@Override
    public void init() {
        this.init(new OrderedThreadPoolExecutor(0, 1000, 30L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), this));
    }
	
	@Override
    public void init(TimerThread timer){
		//Check on cast
    	super.init((OrderedTimerThread)timer);
    }

	/* (non-Javadoc)
	 * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
	 */
	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
		LOG.warn("Task " + r.toString() + " rejected from " + e.toString());
		OrderedTimerTaskWorker worker = (OrderedTimerTaskWorker)r;
		worker.reject(new RejectedTaskReason(RejectedTaskReason.POOL_FULL));
	}
	
}
