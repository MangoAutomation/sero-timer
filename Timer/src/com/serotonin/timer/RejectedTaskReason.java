/**
 * Copyright (C) 2016 Infinite Automation Software. All rights reserved.
 * @author Terry Packer
 */
package com.serotonin.timer;

/**
 * @author Terry Packer
 *
 */
public class RejectedTaskReason {

	public static final int POOL_FULL = 1; //The thread pool's workers are all busy
	public static final int TASK_QUEUE_FULL = 2; //The Executor's queue for this task is at capacity
	public static final int CURRENTLY_RUNNING = 3; //The task is Ordered and one of its worker instances is already running
	
	private int code;
	
	public RejectedTaskReason(int reasonCode){
		this.code = reasonCode;
	}
	
	public int getCode(){
		return code;
	}
	
	public String getDescription(){
		switch(code){
		case POOL_FULL:
			return "Pool Full";
		case TASK_QUEUE_FULL:
			return "Task Queue Full";
		case CURRENTLY_RUNNING:
			return "Task Currently Running";
		default:
			return "Unknown";
		}
	}
	
}
