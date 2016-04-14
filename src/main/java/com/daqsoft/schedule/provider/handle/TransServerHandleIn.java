package com.daqsoft.schedule.provider.handle;

import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.SystemContext;
import com.daqsoft.schedule.pojo.TaskMessage;
import com.daqsoft.schedule.provider.job.queue.JobQueue;
import com.daqsoft.schedule.provider.system.RunningStatus;
import com.daqsoft.schedule.resp.AcceptResponse;
import com.daqsoft.schedule.resp.StatusResponse;
/**
 * Request dispatcher
 * 
 * @author ShawnShoper
 *
 */
@Component
public class TransServerHandleIn
{
	Logger log = LoggerFactory.getLogger(TransServerHandleIn.class);
	@Autowired
	JobQueue jobQueue;

	/**
	 * Receiving task
	 * 
	 * @param taskMessage
	 * @return
	 */
	public AcceptResponse receive(TaskMessage taskMessage)
	{
		AcceptResponse acceptResponse = new AcceptResponse();
		try
		{
			checkArgs(taskMessage);
			acceptResponse.setRespTime(System.currentTimeMillis());
			jobQueue.putPending(taskMessage);
			RunningStatus.serviceTimes.incrementAndGet();
			acceptResponse.setAccepted(true);
		} catch (com.daqsoft.schedule.exception.SystemException e)
		{
			log.warn("During a exception {}", e.getLocalizedMessage(), e);
			acceptResponse.setMessage(e.getLocalizedMessage());
		}
		return acceptResponse;
	}
	/**
	 * 检查参数是否有效值
	 * 
	 * @param taskMessage
	 * @throws com.daqsoft.schedule.exception.SystemException
	 */
	private void checkArgs(TaskMessage taskMessage)
			throws com.daqsoft.schedule.exception.SystemException
	{
		if (taskMessage == null)
			throw new com.daqsoft.schedule.exception.SystemException(
					"TaskMessage can not be null...");
		if (taskMessage.getTask() == null)
			throw new com.daqsoft.schedule.exception.SystemException(
					"Task can not be null...");
		if (taskMessage.getTask().getId() == null)
			throw new com.daqsoft.schedule.exception.SystemException(
					"Task id can not be null...");
		if (taskMessage.getTaskTemplate() == null)
			throw new com.daqsoft.schedule.exception.SystemException(
					"TaskTemplate can not be null...");
	}
	/**
	 * Get system's status
	 * 
	 * @return
	 * @throws TException
	 */
	public StatusResponse getStatus() throws TException
	{
		StatusResponse statusResponse = new StatusResponse();
		statusResponse.setServeTimes(RunningStatus.serviceTimes.get());
		statusResponse.setStartTime(SystemContext.startTime);
		statusResponse.setHoldeCount(jobQueue.getHolder());
		statusResponse.setRespTime(System.currentTimeMillis());
		return statusResponse;
	}

	public List<String> getAllRunning() throws TException
	{
		return jobQueue.getRunning();
	}
}
