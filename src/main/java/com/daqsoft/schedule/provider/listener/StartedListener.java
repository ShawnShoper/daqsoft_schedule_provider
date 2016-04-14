package com.daqsoft.schedule.provider.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.SystemContext;
import com.daqsoft.schedule.conf.ZKInfo;
import com.daqsoft.schedule.module.MongoModule;
import com.daqsoft.schedule.provider.module.HDFSModule;
import com.daqsoft.schedule.provider.module.JobProcesser;
import com.daqsoft.schedule.provider.module.LogModule;
import com.daqsoft.schedule.provider.module.Registrar;
import com.daqsoft.schedule.provider.module.ReportModule;

@Component
public class StartedListener
		implements
			ApplicationListener<ContextRefreshedEvent>
{
	@Autowired
	ZKInfo zkInfo;
	@Autowired
	Registrar registrar;
	@Autowired
	JobProcesser jobProcesser;
	@Autowired
	MongoModule mongoModule;
	@Autowired
	ReportModule reportModule;
	@Autowired
	LogModule logModule;
	@Autowired
	HDFSModule hdfsModule;
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event)
	{
		// 启动 providerScanner 模块
		try
		{
			// reportModule.start();
			logModule.start();
			registrar.start();
			mongoModule.start();
			jobProcesser.start();
			hdfsModule.start();
		} catch (Exception e)
		{
			e.printStackTrace();
			SystemContext.shutdown();
		} finally
		{
			// ....
		}
	}
}
