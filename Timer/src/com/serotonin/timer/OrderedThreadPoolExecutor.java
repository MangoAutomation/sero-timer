/**
 * Copyright (C) 2016 Infinite Automation Software. All rights reserved.
 * @author Terry Packer
 */
package com.serotonin.timer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
* This Executor warrants task ordering for tasks with same key (key have to implement hashCode and equal methods correctly).
*/
public class OrderedThreadPoolExecutor extends ThreadPoolExecutor{

	private final Map<Object, Queue<Runnable>> keyedTasks = new HashMap<Object, Queue<Runnable>>();

	//TODO Implement queue size constraint
	//private int queueSize;
	
    /**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param handler
	 */
	public OrderedThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
	}

	/**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param threadFactory
	 * @param handler
	 */
	public OrderedThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
			RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory, handler);
	}

	/**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param threadFactory
	 */
	public OrderedThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory);
	}

	/**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 */
	public OrderedThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}

	public void execute(OrderedTimerTaskWorker worker, Object key) {
        if (key == null){ // if key is null, execute without ordering
            execute(worker);
            return;
        }

        boolean first;
        Runnable wrappedTask;
        synchronized (keyedTasks){
            Queue<Runnable> dependencyQueue = keyedTasks.get(key);
            first = (dependencyQueue == null);
            if (dependencyQueue == null){
                dependencyQueue = new LinkedList<Runnable>();
                keyedTasks.put(key, dependencyQueue);
            }

            wrappedTask = wrap(worker, dependencyQueue, key);
            if (!first){
            	//TODO Implement logic to check queue size and reject/clear cache if full
                //TODO If the the queue is full, flush queue and add new task to queue

            	worker.reject(new RejectedTaskReason(RejectedTaskReason.CURRENTLY_RUNNING));
                //dependencyQueue.add(wrappedTask);
            }
        }

        // execute method can block, call it outside synchronize block
        if (first)
            execute(wrappedTask);

    }

    private Runnable wrap(Runnable task, Queue<Runnable> dependencyQueue, Object key) {
        return new OrderedTask(task, dependencyQueue, key);
    }

    class OrderedTask implements Runnable{

        private final Queue<Runnable> dependencyQueue;
        private final Runnable task;
        private final Object key;

        public OrderedTask(Runnable task, Queue<Runnable> dependencyQueue, Object key) {
            this.task = task;
            this.dependencyQueue = dependencyQueue;
            this.key = key;
        }

        @Override
        public void run() {
            try{
                task.run();
            } finally {
                Runnable nextTask = null;
                synchronized (keyedTasks){
                    if (dependencyQueue.isEmpty()){
                        keyedTasks.remove(key);
                    }else{
                        nextTask = dependencyQueue.poll();
                    }
                }
                if (nextTask!=null)
                    execute(nextTask);
            }
        }
    }
}