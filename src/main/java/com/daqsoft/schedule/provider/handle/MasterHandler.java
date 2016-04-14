package com.daqsoft.schedule.provider.handle;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.daqsoft.schedule.connect.ProviderConnection;
import com.daqsoft.schedule.exception.SystemException;
import com.daqsoft.schedule.face.ReportServer;

public class MasterHandler
{
	private volatile ReportServer.Client reportClient;
	private volatile ProviderConnection thriftConnection;
	TSocket socket = null;

	public MasterHandler(ProviderConnection thriftConnection)
	{
		this.thriftConnection = thriftConnection;
	}

	public ReportServer.Client getTransServerClient()
	{
		return reportClient;
	}

	public void setTransServerClient(ReportServer.Client transServerClient)
	{
		this.reportClient = transServerClient;
	}
	/**
	 * Connecting remote server...<br>
	 * if connect fail , will try 2 times to reconnect
	 * 
	 * @return server instance
	 * @throws SystemException
	 */
	public ReportServer.Client connect() throws SystemException
	{
		boolean flag = false;
		for (int i = 0; i < 3; i++)
		{
			try
			{
				socket = new TSocket(thriftConnection.getHost(),
						thriftConnection.getPort());
				TBinaryProtocol protocol = new TBinaryProtocol(socket);
				TMultiplexedProtocol mp1 = new TMultiplexedProtocol(protocol,
						thriftConnection.getProvideName());
				reportClient = new ReportServer.Client(mp1);
				socket.open();
				flag = true;
				break;
			} catch (TTransportException e)
			{
				e.printStackTrace();
			}
		}
		if (!flag)
			throw new SystemException("连接" + thriftConnection.getHost() + ":"
					+ thriftConnection.getPort() + "失败.");
		return reportClient;
	}
	/**
	 * 关闭连接...
	 */
	public void close()
	{
		socket.close();
	}

	public int report(String report) throws SystemException
	{
		try
		{
			return connect().reportJobDone(report);
		} catch (TException e)
		{
			// 发送数据或者连接异常,这部分需要捕获处理，如果是连接 master 则只需要重试。

			// 如果是链接的是 slave,那么需要做负载以及重试
			e.printStackTrace();
		}
		return 1;
	}

}
