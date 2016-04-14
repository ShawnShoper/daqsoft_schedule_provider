package com.daqsoft.schedule.provider.face.in;
import java.util.List;

import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.daqsoft.schedule.face.TransServer;
import com.daqsoft.schedule.pojo.TaskMessage;
import com.daqsoft.schedule.provider.handle.TransServerHandleIn;
@Component
public class TransServerIHandler implements TransServer.Iface
{
	@Autowired
	TransServerHandleIn dispatch;
	@Override
	public String sendTask(String taskMessage) throws TException
	{
		TaskMessage tm = TaskMessage.parseObject(taskMessage);
		String response = dispatch.receive(tm).toJson();
		return response;
	}

	@Override
	public String getStatus() throws TException
	{
		String response = dispatch.getStatus().toJson();
		return response;
	}

	@Override
	public List<String> getAllRunning() throws TException
	{
		return dispatch.getAllRunning();
	}

}
