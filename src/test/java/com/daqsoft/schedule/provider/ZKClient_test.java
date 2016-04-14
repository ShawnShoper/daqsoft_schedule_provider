package com.daqsoft.schedule.provider;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.shoper.mail.MailInfo;
import org.shoper.zookeeper.ZKClient;
import org.shoper.zookeeper.ZKWatcher;

import com.daqsoft.schedule.conf.HDFSInfo;
import com.daqsoft.schedule.conf.MongoInfo;
import com.daqsoft.schedule.conf.RedisInfo;

public class ZKClient_test
{
	public static void main(String[] args)
			throws KeeperException, InterruptedException
	{
		ZKClient zkClient = new ZKClient("192.168.100.177", 2181, 50000,
				new MyWatch());
		zkClient.createNode("/daq", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/config", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/config/provider", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/master", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/provider", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/config/master", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/config/redis", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/config/mongo", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/config/hdfs", CreateMode.PERSISTENT);
		zkClient.createNode("/daq/config/mail", CreateMode.PERSISTENT);
		{
			RedisInfo redisInfo = new RedisInfo();
			redisInfo.setHost("192.168.0.82");
			redisInfo.setPassword("daqsoft");
			redisInfo.setPort(6379);
			redisInfo.setTimeout(10000);
			zkClient.editData("/daq/config/redis", redisInfo.toJson());
		}
		{
			MongoInfo mongoInfo = new MongoInfo();
			mongoInfo.setDbName("daq");
			mongoInfo.setServerAddress("192.168.0.82:27017");
			mongoInfo.setTimeout(20000);
			zkClient.editData("/daq/config/mongo", mongoInfo.toJson());
		}
		{
			HDFSInfo hdfsInfo = new HDFSInfo();
			hdfsInfo.setHostKey("fs.defaultFS");
			hdfsInfo.setHostValue("hdfs://192.168.100.178:8020");
			zkClient.editData("/daq/config/hdfs", hdfsInfo.toJson());
		}
		{
			MailInfo mailInfo = new MailInfo();
			mailInfo.setAccount("xieh@daqsoft.com");
			mailInfo.setPassword("Xiehao1993");
			mailInfo.setAuth(true);
			mailInfo.setSmtp("smtp.exmail.qq.com");
			mailInfo.setTo("xiehao3692@vip.qq.com");
			zkClient.editData("/daq/config/mail", mailInfo.toJson());
		}
		zkClient.close();
	}
	static class MyWatch extends ZKWatcher
	{

		@Override
		public void childrenNodeChangeProcess(WatchedEvent event)
		{
			System.out.println("childrenNodeChangeProcess");
		}

		@Override
		public void dataChangeProcess(WatchedEvent event)
		{
			System.out.println("dataChangeProcess");
		}

		@Override
		public void nodeDeleteProcess(WatchedEvent event)
		{
			System.out.println("nodeDeleteProcess");
		}

		@Override
		public void nodeCreateProcess(WatchedEvent event)
		{
			String path = event.getPath();
			path = path.substring(path.lastIndexOf("/") + 1);
			System.out.println(path);
			System.out.println("nodeCreateProcess");
		}
	}
}
