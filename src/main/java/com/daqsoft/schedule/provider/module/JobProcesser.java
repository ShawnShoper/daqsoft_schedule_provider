package com.daqsoft.schedule.provider.module;

import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.shoper.concurrent.future.AsynCallable;
import org.shoper.concurrent.future.AsynRunnable;
import org.shoper.concurrent.future.FutureCallback;
import org.shoper.concurrent.future.FutureManager;
import org.shoper.dynamiccompile.JDKCompile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.conf.ApplicationInfo;
import com.daqsoft.schedule.conf.ProviderInfo;
import com.daqsoft.schedule.exception.SystemException;
import com.daqsoft.schedule.job.JobParam;
import com.daqsoft.schedule.job.JobResult;
import com.daqsoft.schedule.module.MongoModule;
import com.daqsoft.schedule.module.StartableModule;
import com.daqsoft.schedule.pojo.TaskMessage;
import com.daqsoft.schedule.provider.job.JobCaller;
import com.daqsoft.schedule.provider.job.queue.JobQueue;
import com.daqsoft.schedule.provider.job.queue.ReportQueue;
import com.daqsoft.schedule.provider.system.RunningStatus;
import com.daqsoft.schedule.resp.ReportResponse;
import com.daqsoft.schedule.resp.ReportResponse.Error;
import com.daqsoft.schedule.resp.ResultResponse;

@Component
public class JobProcesser extends StartableModule
{
	@Autowired
	ApplicationInfo appInfo;
	@Autowired
	ReportModule reportModule;
	@Autowired
	HDFSModule hdfsModule;

	private Logger log = LoggerFactory.getLogger(JobProcesser.class);
	@PostConstruct
	public void init()
	{
		log.info("Job processer 	initializing");

	}
	@PreDestroy
	public void destroy()
	{
		stop();
	}
	@Autowired
	JobQueue jobQueue;
	@Override
	public int start()
	{
		FutureManager.pushFuture(appInfo.getGroup(), "jobProcess",
				new AsynCallable<Boolean>() {
					@Override
					public Boolean run() throws Exception
					{
						try
						{
							for (;;)
							{
								jobQueue.runnabledLock(RunningStatus.limitTask);
								TaskMessage taskMessage = jobQueue
										.takePending();
								FutureManager.pushFuture(appInfo.getGroup(),
										"task-handle-"
												+ taskMessage.getTask().getId(),
										new AsynRunnable() {
											@Override
											public void call() throws Exception
											{
												if (taskMessage != null)
													toJobRunner(taskMessage);
											}
										});
							}
						} catch (InterruptedException e)
						{
							;
						}
						return true;
					}
				});
		super.setStarted(true);
		return 0;
	}
	@Autowired
	MongoModule mongoModule;
	@Autowired
	LogModule logModule;
	private void toJobRunner(TaskMessage taskMessage)
	{
		try
		{
			byte[] code = taskMessage.getTaskTemplate().getCode();
			String param = taskMessage.getTask().getParams();
			JobParam argsTmp = null;
			// 如果该任务没有任何参数,不在反序列化
			{
				if (param == null || param.isEmpty())
					argsTmp = new JobParam();
				else
					argsTmp = JobParam.parseObject(param);
			}
			if (argsTmp.getCookies() == null)
			{
				argsTmp.setCookies(taskMessage.getTask().getCookies());
			}
			JobParam args = argsTmp;
			JobCaller jobCaller = getInstance(new String(code));
			JobResult jobResult = jobCaller.getJobResult();
			// 监听....
			Timer timer = new Timer(true);
			FutureManager.pushFuture(appInfo.getName(),
					"JobRunner" + taskMessage.getTask().getId(),
					new AsynCallable<JobResult>() {
						@Override
						public JobResult run() throws Exception
						{
							return jobCaller.run();
						}
					}.setCallback(new FutureCallback<JobResult>() {
						@Override
						protected boolean preDo()
						{
							jobResult.setJobName(
									taskMessage.getTask().getName());
							jobResult.setStartTime(System.currentTimeMillis());
							timer.schedule(new TimerTask() {
								@Override
								public void run()
								{
									// Do save 日志..
									ResultResponse response = new ResultResponse(
											appInfo.getBindAddr() + ":"
													+ appInfo.getPort(),
											jobResult.getJobName(),
											jobResult.isDone(),
											jobResult.getSaveCount().get(),
											jobResult.isSuccess(),
											jobResult.getUpdateCount().get(),
											jobResult.getHandleCount().get(),
											jobResult.getStartTime(),
											jobResult.getEndTime(),
											System.currentTimeMillis()
													- jobResult.getStartTime());
									logModule.log("progress",
											response.toJson());
								}
							}, 0, 2000);
							jobCaller.setMongoModule(mongoModule);
							jobCaller.setHdfsModule(hdfsModule);
							return jobCaller.init(args);
						}

						@Override
						protected void success(JobResult result)
						{
							log.info("{} job has be success.",
									taskMessage.getTask().getId());
							RunningStatus.successTimes.incrementAndGet();
							// 发送通知给队列...
							jobOver(taskMessage.getTask().getId(), jobResult,
									null);
						}
						@Override
						protected void fail(Exception e)
						{
							log.info("{} job has be failed.",
									taskMessage.getTask().getId());
							RunningStatus.failedTimes.incrementAndGet();
							e.printStackTrace();
							jobOver(taskMessage.getTask().getId(), null,
									Error.FETAL);
						}
						@Override
						protected void done()
						{
							jobResult.setEndTime(System.currentTimeMillis());
							log.info("{} job has be done.destroying",
									taskMessage.getTask().getId());
							ResultResponse response = new ResultResponse(
									appInfo.getBindAddr() + ":"
											+ appInfo.getPort(),
									jobResult.getJobName(), jobResult.isDone(),
									jobResult.getSaveCount().get(),
									jobResult.isSuccess(),
									jobResult.getUpdateCount().get(),
									jobResult.getHandleCount().get(),
									jobResult.getStartTime(),
									jobResult.getEndTime(),
									System.currentTimeMillis()
											- jobResult.getStartTime());
							logModule.log("progress", response.toJson());
							timer.cancel();
							jobCaller.destroy();
						}
					}));
		} catch (Exception e1)
		{
			jobOver(taskMessage.getTask().getId(), null, Error.FETAL);
			RunningStatus.failedTimes.incrementAndGet();
			e1.printStackTrace();
		}
		/*
		 * } catch (ClassNotFoundException | InstantiationException |
		 * IllegalAccessException e) { //这里的 catch 似乎有些脑残啊
		 * jobQueue.removeRunning(taskMessage.getTask().getId()); ReportResponse
		 * reportResponse = new ReportResponse();
		 * reportResponse.setErr(Error.FETAL);
		 * reportResponse.setRespTime(System.currentTimeMillis());
		 * jobQueue.putReport(reportResponse); e.printStackTrace(); }
		 */
	}
	/**
	 * 实例化源码并转成 jobCaller
	 * 
	 * @param code
	 * @return
	 * @throws SystemException
	 */
	JobCaller getInstance(String code) throws SystemException
	{
		Class<?> clazz;
		JobCaller jobCaller;
		try
		{
			clazz = JDKCompile.getClass(code);
			jobCaller = (JobCaller) clazz.newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException | org.shoper.SystemException e)
		{
			throw new SystemException(e.getLocalizedMessage());
		}
		return jobCaller;
	}
	@Autowired
	ProviderInfo providerInfo;
	/**
	 * 任务结束
	 * 
	 * @param id
	 * @param jobResult
	 * @param error
	 */
	public void jobOver(String id, JobResult jobResult, Error error)
	{
		jobQueue.removeRunning(id);
		ReportResponse reportResponse = new ReportResponse();
		reportResponse.setProviderKey(appInfo.getBindAddr()
				+ providerInfo.getPort() + providerInfo.getVersion());
		reportResponse.setGroup(providerInfo.getGroup());
		reportResponse.setJob(id);
		if (error != null)
			reportResponse.setErr(error);
		if (jobResult != null)
			reportResponse.setJobResult(jobResult);
		ReportQueue.putReport(reportResponse);
	}
	@Override
	public void stop()
	{
		FutureManager.futureDone(appInfo.getName(), "jobProcess");
		log.info("Job processer 	destroying");
		setStarted(false);
	}
}
