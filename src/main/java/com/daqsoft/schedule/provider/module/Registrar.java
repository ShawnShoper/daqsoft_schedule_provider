package com.daqsoft.schedule.provider.module;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.hyperic.sigar.SigarException;
import org.shoper.system.SystemUtil;
import org.shoper.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.SystemContext;
import com.daqsoft.schedule.conf.ApplicationInfo;
import com.daqsoft.schedule.conf.ProviderInfo;
import com.daqsoft.schedule.conf.ZKInfo;
import com.daqsoft.schedule.connect.ProviderConnection;
import com.daqsoft.schedule.connect.ProviderURLBuilder;
import com.daqsoft.schedule.exception.SystemException;
import com.daqsoft.schedule.face.TransServer;
import com.daqsoft.schedule.manager.ZKModule;
import com.daqsoft.schedule.provider.job.queue.JobQueue;
import com.daqsoft.schedule.provider.system.RunningStatus;
import com.daqsoft.schedule.resp.StatusResponse;
@Component
public class Registrar extends ZKModule
{
	private Logger log = LoggerFactory.getLogger(Registrar.class);
	@Autowired
	ApplicationInfo appInfo;
	@Autowired
	private ZKInfo zkInfo;
	@Autowired
	private ThriftStarter thriftStarter;
	@Autowired
	private ProviderInfo providerInfo;
	@PostConstruct
	public void init()
	{
		setZkInfo(zkInfo);
	}
	@PreDestroy
	public void destroy()
	{
		stop();
		log.debug("Registrar destroy");
	}
	@Override
	public void stop()
	{
		super.stop();
	}

	@Override
	public int start()
	{
		try
		{
			if (super.start() == 1)
				return 1;
			startThrift();
			registry();
			setStarted(true);
		} catch (InterruptedException | KeeperException e)
		{
			return 1;
		}
		return 0;
	}
	/**
	 * 构建 zk 节点.
	 * 
	 * @return
	 */
	String builderMonitorNode()
	{
		ProviderConnection tc = new ProviderConnection();
		tc.setGroup(providerInfo.getGroup());
		tc.setHost(appInfo.getBindAddr());
		tc.setPort(providerInfo.getPort());
		tc.setProvideName(TransServer.class.getName());
		tc.setTimeout(providerInfo.getTimeout());
		tc.setUnit(TimeUnit.SECONDS);
		tc.setVersion(providerInfo.getVersion());
		return providerInfo.getNodePath() + "/"
				+ StringUtil.urlEncode(ProviderURLBuilder.Builder().build(tc));
	}

	@Override
	public void nodeDeleteProcess(WatchedEvent event)
	{
		// 防止节点被外接删除...
		try
		{
			registry();
		} catch (KeeperException e)
		{
			e.printStackTrace();
		} catch (InterruptedException e)
		{
			;
		}
	}
	void createMonitorNode() throws KeeperException, InterruptedException
	{
		super.getZkClient().createNode(builderMonitorNode(), "",
				CreateMode.EPHEMERAL);
	}
	@Autowired
	JobQueue jobQueue;
	Timer timer = new Timer();
	/**
	 * Registry zookeeper
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private void registry() throws KeeperException, InterruptedException
	{

		String path = builderMonitorNode();
		if (super.getZkClient().exists(path))
			super.getZkClient().deleteNode(path);
		createMonitorNode();
		// 往注册的节点写入当前机器的状态...
		timer.schedule(new TimerTask() {

			@Override
			public void run()
			{

				try
				{
					StatusResponse statusResponse = new StatusResponse();
					statusResponse
							.setServeTimes(RunningStatus.serviceTimes.get());
					statusResponse.setStartTime(SystemContext.startTime);
					statusResponse.setHoldeCount(jobQueue.getHolder());
					statusResponse.setRespTime(System.currentTimeMillis());
					try
					{
						statusResponse
								.setPriority(new BigDecimal(Double
										.valueOf(SystemUtil.getMemInfo()
												.getFree())
										/ 1024 / 1024 / 1024 + "")
												.setScale(2,
														RoundingMode.HALF_EVEN)
												.add(new BigDecimal(
														RunningStatus.cpuWeight))
												.doubleValue());
					} catch (NumberFormatException | SigarException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					statusResponse.setCpuIdlePercent(
							RunningStatus.getCpuIdlePercent());
					statusResponse.setMemUsedPercent(
							RunningStatus.getMemUsedPercent());
					Registrar.this.getZkClient().editData(builderMonitorNode(),
							statusResponse.toJson().getBytes());
				} catch (SystemException e)
				{
					e.printStackTrace();
				} catch (KeeperException e)
				{
					e.printStackTrace();
				} catch (InterruptedException e)
				{
					;
				}
			}
		}, 0, appInfo.getHeartBeat());
	}
	/**
	 * start thrift
	 * 
	 * @throws InterruptedException
	 */
	private void startThrift() throws InterruptedException
	{
		thriftStarter.start(appInfo.getBindAddr(), providerInfo.getPort());
		while (!thriftStarter.isStarted())
		{
			TimeUnit.MILLISECONDS.sleep(10);
		}
		log.info("Thrift started...");
	}
	@Override
	protected void sessionExpired()
	{
		super.sessionExpired();
		super.startZookeeper();
		for (;;)
			try
			{
				registry();
				break;
			} catch (KeeperException e)
			{
				e.printStackTrace();
			} catch (InterruptedException e)
			{
				break;
			}
	}

}
