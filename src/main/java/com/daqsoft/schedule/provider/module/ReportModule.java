package com.daqsoft.schedule.provider.module;

import java.net.URLDecoder;

import javax.annotation.PostConstruct;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.shoper.concurrent.future.AsynCallable;
import org.shoper.concurrent.future.FutureManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.conf.ApplicationInfo;
import com.daqsoft.schedule.conf.ZKInfo;
import com.daqsoft.schedule.connect.ProviderConnection;
import com.daqsoft.schedule.connect.ProviderURLBuilder;
import com.daqsoft.schedule.exception.SystemException;
import com.daqsoft.schedule.manager.ZKModule;
import com.daqsoft.schedule.provider.handle.MasterConnector;
import com.daqsoft.schedule.provider.job.queue.ReportQueue;
import com.daqsoft.schedule.resp.ReportResponse;
/**
 * 任务报告模块<br>
 * 用于执行任务关闭后的报告信息..
 * 
 * @author ShawnShoper
 *
 */
@Component
public class ReportModule extends ZKModule
{
	private MasterConnector masterConnector = null;
	public MasterConnector getMasterConnector()
	{
		return masterConnector;
	}
	Logger LOGGER = LoggerFactory.getLogger(ReportModule.class);
	@Autowired
	ApplicationInfo appInfo;
	@Autowired
	ZKInfo zkInfo;
	@PostConstruct
	public void init()
	{
		setZkInfo(zkInfo);
	}
	@Override
	public int start()
	{
		try
		{
			if (super.start() == 1)
				return 1;
			masterConnector = initMaster();
			if (masterConnector == null)
			{
				return 1;
			}
			startReportDaemon();
			setStarted(true);
		} catch (InterruptedException | SystemException e)
		{
			return 1;
		}
		return 0;
	}
	/**
	 * 启动任务汇报线程...
	 */
	private void startReportDaemon()
	{
		FutureManager.pushFuture(appInfo.getName(), "report",
				new AsynCallable<Boolean>() {

					@Override
					public Boolean run() throws Exception
					{
						for (;;)
						{
							ReportResponse report = ReportQueue.takeReport();
							try
							{
								masterConnector.getThriftHandler()
										.report(report.toJson());
							} catch (SystemException e)
							{
								// 自行处理 SystemException
								LOGGER.info(
										"[ReportModule]:任务汇报失败,重入汇报队列,等待下次汇报...");
								ReportQueue.putReport(report);
							}
						}
					}
				});
	}
	/**
	 * 扫描provider
	 * 
	 * @throws InterruptedException
	 * @throws SystemException
	 */
	private MasterConnector initMaster()
			throws InterruptedException, SystemException
	{
		MasterConnector masterConnector = null;
		try
		{
			byte[] data = super.getZkClient().showData(appInfo.getMasterPath());
			if (data == null)
				LOGGER.info(
						"[ReportModule]:未获取调度端节点数据...可能原因:调度端未启动...网络不通...");
			String master = new String(data);
			ProviderConnection tc = ProviderURLBuilder.Builder()
					.deBuild(URLDecoder.decode(master));
			masterConnector = initConnector(tc);
		} catch (KeeperException e)
		{
			e.printStackTrace();// Do nothing...
		}
		return masterConnector;
	}
	MasterConnector initConnector(ProviderConnection tc) throws SystemException
	{
		LOGGER.info("[ReportModule]:检查调度端是否可用...");
		MasterConnector mc = MasterConnector.buider(tc);
		try
		{
			mc.getThriftHandler().connect();
		} catch (SystemException e)
		{
			LOGGER.info("[ReportModule]:调度端连接失败.", e);
			throw new SystemException(e.getLocalizedMessage());
		}
		return mc;
	}

	@Override
	public void dataChangeProcess(WatchedEvent event)
	{
		try
		{
			masterConnector = initMaster();
		} catch (InterruptedException e)
		{
			;// Do nothing...
		} catch (SystemException e)
		{
			e.printStackTrace();
		}
	}

}
