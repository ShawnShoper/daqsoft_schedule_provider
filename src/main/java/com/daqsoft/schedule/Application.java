package com.daqsoft.schedule;

import org.apache.thrift.transport.TTransportException;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.daqsoft.schedule")
public class Application
{
	public static void main(String[] args) throws TTransportException
	{
		ConfigurableApplicationContext context = null;
		try
		{
			context = new SpringApplicationBuilder().sources(Application.class)
					.bannerMode(Banner.Mode.OFF).sources(Application.class)
					.run(args);
			context.registerShutdownHook();
			SystemContext.context = context;
		} catch (Exception e)
		{
			SystemContext.shutdown();
		}
		SystemContext.waitShutdown();
		System.out.println("shutdown");
	}
}
