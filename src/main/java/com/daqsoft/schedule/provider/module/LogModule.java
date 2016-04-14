package com.daqsoft.schedule.provider.module;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.zookeeper.KeeperException;
import org.shoper.redis.RedisClient;
import org.shoper.redis.RedisPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.conf.ApplicationInfo;
import com.daqsoft.schedule.conf.RedisInfo;
import com.daqsoft.schedule.conf.ZKInfo;
import com.daqsoft.schedule.manager.ZKModule;

@Component
public class LogModule extends ZKModule
{
	@Autowired
	private ZKInfo zkInfo;
	@Autowired
	private ApplicationInfo appInfo;

	RedisClient redisClient;
	@PostConstruct
	public void init()
	{
		super.setZkInfo(zkInfo);
	}
	@PreDestroy
	public void destory()
	{
		stop();

	}

	@Override
	public void stop()
	{
		super.stop();
		redisClient.close();
	}
	@Override
	public int start()
	{
		if (super.start() == 1)
			return 1;
		// Initialize redis info....
		try
		{
			RedisInfo redisInfo = readData();
			initial(redisInfo);
		} catch (KeeperException | InterruptedException e)
		{
			return 1;
		}
		setStarted(true);
		return 0;
	}

	private void initial(RedisInfo redisInfo)
	{
		redisClient = RedisPool
				.newInstances(redisInfo.getHost(), redisInfo.getPort(),
						redisInfo.getTimeout(), redisInfo.getPassword())
				.getRedisClient();
	}

	private RedisInfo readData() throws KeeperException, InterruptedException
	{
		byte[] data = super.getZkClient().showData(appInfo.getRedisPath());
		String info = new String(data);
		return RedisInfo.parseObject(info);
	}

	public RedisClient getLogger()
	{
		return redisClient;
	}
	public void log(String key, String log)

	{
		getLogger().tpush(key, log);

	}
}
