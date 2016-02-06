/**
 * Copyright (C) 2016 Infinite Automation Software. All rights reserved.
 * @author Terry Packer
 */
package com.serotonin.timer;


/**
 * @author Terry Packer
 *
 */
class OrderedTimerThread extends TimerThread{
 
	private final OrderedThreadPoolExecutor executorService;

    OrderedTimerThread(TaskQueue queue, OrderedThreadPoolExecutor executorService, TimeSource timeSource) {
        super(queue, executorService, timeSource);
        this.executorService = executorService;
    }


    /* (non-Javadoc)
     * @see com.serotonin.timer.TimerThread#executeTask(com.serotonin.timer.TimerTask)
     */
    @Override
    void executeTask(TimerTask task, long executionTime) {
        executorService.execute(new OrderedTimerTaskWorker(task, executionTime), task.getTaskId());
    }
}
