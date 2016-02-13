/**
 * Copyright (C) 2016 Infinite Automation Software. All rights reserved.
 * @author Terry Packer
 */
package com.serotonin.timer;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
* This Executor warrants task ordering for tasks with same id.
* 
* Default ID for TimerTasks is hasCode()
* 
* Generally periodic tasks will be scheduled and run as the same object, meaning that only one of them 
* can run at any given time and the first must finish before the next can run.  Single run tasks will always be 
* different instances if they are created when they are executed, so multiple instances can run at the same time.
* 
* The optional defaultQueueSize parameter will allow multiple tasks with the same ID to be queued up and run in order.  The default queue size of 0 
* indicates that no tasks can be queued and that tasks submitted while another is running will be rejected with RejectedTaskReason.TASK_QUEUE_FULL
* 
* The flushFullQueue parameter can be used to force a queue to flush its pending tasks and replace them with the incoming task.  This is useful 
* if pending tasks are deemed out-dated and become irrelevant where by running the most recenty scheduled task is preferred to executing all old stale tasks.
* 
* Note that every queue will be removed once it is empty.  
* 
*/
public class OrderedThreadPoolExecutor extends ThreadPoolExecutor implements RejectedExecutionHandler{

	//Task to queue map
	private final Map<Object, LimitedTaskQueue> keyedTasks = new HashMap<Object, LimitedTaskQueue>();

	//Default size for all queues with task IDs not in the below map
	private int defaultQueueSize;
	private boolean flushFullQueue;
	private RejectedExecutionHandler handler;
	
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
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		this.defaultQueueSize = 0;
		this.flushFullQueue = false;
		super.setRejectedExecutionHandler(this);
		this.handler = handler;
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
				threadFactory);
		super.setRejectedExecutionHandler(this);
		this.handler = handler;
		this.defaultQueueSize = 0;
		this.flushFullQueue = false;
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
		this.defaultQueueSize = 1;
		this.flushFullQueue = false;
	}

	/**
	 * 
	 * Overloaded constructor to allow tuning the task queues
	 * 
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param threadFactory
	 * @param defaultQueueSize
	 * @param flushFullQueue
	 */
	public OrderedThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler, 
			int defaultQueueSize, boolean flushFullQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory);
		this.defaultQueueSize = defaultQueueSize;
		this.flushFullQueue = flushFullQueue;
		super.setRejectedExecutionHandler(this);
		this.handler = handler;
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
		this.defaultQueueSize = 0;
		this.flushFullQueue = false;
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.ThreadPoolExecutor#setRejectedExecutionHandler(java.util.concurrent.RejectedExecutionHandler)
	 */
	@Override
	public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
		this.handler = handler;
	}
	
	
	public void execute(OrderedTimerTaskWorker worker, Object key) {
        if (key == null){ // if key is null, execute without ordering
            execute(worker);
            return;
        }

        boolean first,added=false;
        OrderedTask wrappedTask;
        LimitedTaskQueue dependencyQueue = null;
        
        synchronized (keyedTasks){
        	dependencyQueue = keyedTasks.get(key);
            first = (dependencyQueue == null);
            if (dependencyQueue == null){
            	if(flushFullQueue)
            		dependencyQueue = new TimePriorityLimitedTaskQueue(this.defaultQueueSize);
            	else
            		dependencyQueue = new LimitedTaskQueue(this.defaultQueueSize);
                keyedTasks.put(key, dependencyQueue);
            }

            wrappedTask = wrap(worker, dependencyQueue, key);
            if(!first)
            	added = dependencyQueue.add(wrappedTask);
        }

        // execute and reject methods can block, call them outside synchronize block
        if (first)
            execute(wrappedTask);
        else if(!added){
        	OrderedTask t = dependencyQueue.getRejectedTasks().poll();
        	
        	while(t != null){
        		t.reject(this);
        		t = dependencyQueue.getRejectedTasks().poll();
        	}
        }
        	

    }

	/** 
	 * We need to ensure we remove the keyed tasks if we get rejected
	 * 
	 * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
	 */
	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
		
		if(r instanceof OrderedTask){
			synchronized (keyedTasks){
				//Don't bother trying to run the queue we've got a problem
				OrderedTask t  = (OrderedTask)r;
				if (t.dependencyQueue.isEmpty()){
                    keyedTasks.remove(t.key);
                }else{
                	//Could be trouble, but let it fail if it must
                    //execute(t.dependencyQueue.poll());
                }
			}
		}
		
		if(this.handler != null){
			//Pass it along
			this.handler.rejectedExecution(r, e);
		}else{
			//Default Behavior
			throw new RejectedExecutionException("Task " + r.toString() +
                    " rejected from " +
                    e.toString());
		}
	}
	
	/**
	 * Update the default queue size for a given taskId
	 * @param newSize
	 * @param taskId
	 */
	public void updateDefaultQueueSize(int newSize, Object taskId){
		synchronized (keyedTasks){
			LimitedTaskQueue dependencyQueue = keyedTasks.get(taskId);
            if (dependencyQueue == null){
                dependencyQueue = new LimitedTaskQueue(this.defaultQueueSize);
                keyedTasks.put(taskId, dependencyQueue);
            }
            dependencyQueue.setLimit(newSize);
		}
	}
	
	/**
	 * Test to see if a queue for this task id exists
	 * @param taskId
	 * @return
	 */
	public boolean queueExists(Object taskId){
		synchronized (keyedTasks){
			return keyedTasks.containsKey(taskId);
		}
	}
	
    private OrderedTask wrap(OrderedTimerTaskWorker task, LimitedTaskQueue dependencyQueue, Object key) {
        return new OrderedTask(task, dependencyQueue, key);
    }

    /**
     * Class that ensures tasks of the same type are run in order
     * 
     * When all tasks are run and removed from dependencyQueue 
     * the entire queue is removed from the keyed tasks map
     *
     */
    class OrderedTask implements Runnable{

        private final LimitedTaskQueue dependencyQueue;
        private final OrderedTimerTaskWorker task;
        private final Object key;
        private int rejectedReason;

        public OrderedTask(OrderedTimerTaskWorker task, LimitedTaskQueue dependencyQueue, Object key) {
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
        
        public void setRejectedReason(int reason){
        	this.rejectedReason = reason;
        }
        
        public void reject(Executor e){
        	this.task.reject(new RejectedTaskReason(this.rejectedReason, task.getExecutionTime(), this.task.getTask(), e));
        }
        
        @Override
        public String toString(){
        	return this.task.toString();
        }
    }
    
    /**
     * Class to hold a limited size queue and reject incoming tasks 
     * if the limit is reached false is returned from the add method.
     * 
     * This class ensures that all submitted tasks that are not rejected 
     * will run.  Which is different to TimePriorityLimitedTaskQueue where incoming tasks to a full queue
     * cause the queue to be cleared and the incoming task to take precedence.
     * 
     * @author Terry Packer
     *
     */
    class LimitedTaskQueue extends ArrayDeque<OrderedTask>{

		private static final long serialVersionUID = 1L;
		protected int limit;
		protected ArrayDeque<OrderedTask> rejectedTasks;
		
		public LimitedTaskQueue(int limit){
			super();
			this.limit = limit;
			this.rejectedTasks = new ArrayDeque<OrderedTask>(limit);
		}
		
		/*
		 * (non-Javadoc)
		 * @see java.util.ArrayDeque#add(java.lang.Object)
		 */
		@Override
		public boolean add(OrderedTask e) {
			//Add task to end of queue if there is room
			if(this.size() < limit)
				return super.add(e);
			else{
				e.setRejectedReason(RejectedTaskReason.CURRENTLY_RUNNING);
				this.rejectedTasks.add(e);
				return false;
			}
		}
		
		/**
		 * Useful to get the rejected tasks outside of the sync block where they are added
		 * @return
		 */
		public Queue<OrderedTask> getRejectedTasks(){
			return this.rejectedTasks;
		}
		
		public void setLimit(int limit){
			this.limit = limit;
		}
    }
    
    /**
     * Class to hold a limited size queue and replace entire queue with incoming task if the queue is full.
     * 
     * This class ensures that all submitted tasks that are not rejected 
     * will run.  Which is different to LimitedTaskQueue where incoming tasks to a full queue
     * are rejected.
     * 
     * @author Terry Packer
     *
     */
    class TimePriorityLimitedTaskQueue extends LimitedTaskQueue{

		private static final long serialVersionUID = 1L;
		
		public TimePriorityLimitedTaskQueue(int limit){
			super(limit);
		}
		
		/* (non-Javadoc)
		 * @see java.util.ArrayDeque#add(java.lang.Object)
		 */
		@Override
		public boolean add(OrderedTask e) {
			//Add task to end of queue if there is room
			if(this.size() < limit)
				return super.add(e);
			else{
				while(this.size() >= limit){
					OrderedTask t = this.poll();
					t.setRejectedReason(RejectedTaskReason.TASK_QUEUE_FULL);
					this.rejectedTasks.add(t);
				}
				return false;
			}
		}
		
		public void setLimit(int limit){
			this.limit = limit;
		}
    }
    
}