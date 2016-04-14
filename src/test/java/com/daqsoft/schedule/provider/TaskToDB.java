package com.daqsoft.schedule.provider;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.daqsoft.schedule.job.JobParam;
import com.daqsoft.schedule.pojo.Task;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class TaskToDB
{
	MongoClient mongoClient = null;
	DBCollection collection = null;
	List<Task> data = new ArrayList<>();

	@Before
	public void init() throws Exception
	{
		mongoClient = new MongoClient(new ServerAddress("192.168.0.24", 27017));
		collection = mongoClient.getDB("daq").getCollection("task");
		{
			Task task = new Task();
			task.setCookies(null);
			task.setCreateTime(new Date());
			task.setFailedCount(0);
			task.setCronExpress(null);
			task.setDisabled(false);
			task.setLastFinishTime(new Date(0));
			task.setLoops(0);
			task.setName("百度旅游点评模版-卧龙");
			task.setTemplateID("56c405d67d841a3d64d4504e");
			JobParam jobParam = new JobParam();
			jobParam.setJobCode("0e0d4b57f9374db66f54e1dc");
			jobParam.setJobName("卧龙点评");
			task.setParams(JSONObject.toJSONString(jobParam));
			task.setTiming(false);
			task.setUrl("http://lvyou.baidu.com/wolong/remark/");
			data.add(task);
		}
	}
	public static void main(String[] args)
	{
		JobParam jobParam = new JobParam();
		jobParam.setJobCode("0e0d4b57f9374db66f54e1dc");
		jobParam.setJobName("卧龙点评");
		System.out.println(JSONObject.toJSONString(jobParam));
	}
	@Test
	public void save() throws Exception
	{
		for (Task task : data)
		{
			DBObject query = new BasicDBObject();
			query.put("name", task.getName());
			DBObject dbObject = collection.findOne(query);
			if (dbObject == null)
			{
				// save
				DBObject save = new BasicDBObject();
				save.put("name", task.getName());
				save.put("createTime", task.getCreateTime());
				save.put("url", task.getUrl());
				save.put("cookies", task.getCookies());
				save.put("lastFinishTime", task.getLastFinishTime());
				save.put("cronExpress", task.getCronExpress());
				save.put("params", task.getParams());
				save.put("timing", task.isTiming());
				save.put("failedCount", task.getFailedCount());
				save.put("templateID", task.getTemplateID());
				save.put("loops", task.getLoops());
				save.put("disabled", task.isDisabled());
				collection.save(save);
			}
		}
	}
}
