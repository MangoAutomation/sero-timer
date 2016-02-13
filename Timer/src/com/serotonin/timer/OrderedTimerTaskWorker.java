/**
 * Copyright (C) 2016 Infinite Automation Software. All rights reserved.
 * @author Terry Packer
 */
package com.serotonin.timer;

/**
 * Wrapper to allow running an ordered task with its desired 
 * execution time attached.
 * 
 * @author Terry Packer
 *
 */
public class OrderedTimerTaskWorker implements Runnable{
	
	private TimerTask task;
	private long executionTime;
	
	public OrderedTimerTaskWorker(TimerTask task, long executionTime){
		this.task = task;
		this.executionTime = executionTime;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		this.task.runTask(executionTime);
	}
	
	public void reject(RejectedTaskReason reason){
		this.task.rejected(reason);
	}

	public TimerTask getTask(){
		return this.task;
	}

	public long getExecutionTime(){
		return this.executionTime;
	}
	
	@Override
	public String toString(){
		return this.task.toString();
	}
}
