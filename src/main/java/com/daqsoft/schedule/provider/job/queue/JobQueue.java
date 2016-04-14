package com.daqsoft.schedule.provider.job.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.exception.SystemException;
import com.daqsoft.schedule.pojo.TaskMessage;
/**
 * Job queue.<br>
 * Storing some job that need be processed
 * 
 * @author ShawnShoper
 */
@Component
public class JobQueue
{
	private Logger logger = LoggerFactory.getLogger(JobQueue.class);
	/**
	 * Pending queue for storing job
	 */
	private volatile BlockingDeque<TaskMessage> pendingQueue = new LinkedBlockingDeque<TaskMessage>();
	/**
	 * Running queue
	 */
	private volatile ConcurrentMap<String, TaskMessage> runningQueue = new ConcurrentHashMap<String, TaskMessage>();

	/**
	 * Getting holder job count
	 * 
	 * @return
	 */
	public int getHolder()
	{
		return runningQueue.size() + pendingQueue.size();
	}
	public void addRunning(String key, TaskMessage value)
	{
		runningQueue.putIfAbsent(key, value);
	}

	public void removeRunning(String key)
	{
		if (runningQueue.containsKey(key))
		{
			runningQueue.remove(key);
			notifyRun();
		}
	}

	/**
	 * put a task message into queue. and waiting to be processed
	 * 
	 * @param tm
	 * @throws SystemException
	 *             if can't put in the 'pendQueue'.
	 */
	public void putPending(TaskMessage tm) throws SystemException
	{
		logger.info("Puting a pending task {}", tm);
		try
		{
			if (!pendingQueue.offer(tm, 1, TimeUnit.SECONDS))
			{
				throw new SystemException(
						"Queue is full,can not be put any element in.");
			}
		} catch (InterruptedException e)
		{
			;
		}
	}
	/**
	 * task a pending job.blocking if queue empty
	 * 
	 * @return
	 */
	public TaskMessage takePending()
	{
		logger.info("Requesting a pending task ");
		TaskMessage taskMessage = null;
		retryLoop : for (;;)
		{
			try
			{
				taskMessage = pendingQueue.poll(1, TimeUnit.MINUTES);
				if (null != taskMessage)
					break retryLoop;
			} catch (InterruptedException e)
			{
				break retryLoop;
			}
		}
		logger.info("Taking a pending task {}", taskMessage);
		return taskMessage;
	}
	public List<String> getRunning()
	{
		List<String> running = new ArrayList<>();
		if (!runningQueue.isEmpty())
		{
			running.addAll(runningQueue.keySet());
		}
		return running;
	}
	public int getRunningSize()
	{
		return runningQueue.size();
	}

	private void notifyRun()
	{
		synchronized (this)
		{
			this.notify();
		}
	}
	private void waitRun() throws InterruptedException
	{
		synchronized (this)
		{
			this.wait();
		}
	}
	/**
	 * 设置执行任务的最大数量，如果超过那么阻塞队列进入
	 * 
	 * @param limit
	 * @throws InterruptedException
	 */
	public void runnabledLock(int limit) throws InterruptedException
	{
		if (getRunningSize() >= limit)
			waitRun();
	}
}
