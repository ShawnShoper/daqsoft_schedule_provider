package com.daqsoft.schedule.provider.handle;

import com.daqsoft.schedule.connect.ProviderConnection;

/**
 * Thrift connector...
 * 
 * @author ShawnShoper
 *
 */
public class MasterConnector
{
	private ProviderConnection thriftConnection;
	private MasterHandler thriftHandler;
	private boolean disabled;
	public void reset()
	{

	}

	private MasterConnector()
	{
	}

	public static MasterConnector buider(ProviderConnection tc)
	{
		MasterConnector thriftConnector = new MasterConnector();
		thriftConnector.setConnecion(tc);
		MasterHandler thriftHandler = new MasterHandler(tc);
		thriftConnector.setThriftHandler(thriftHandler);
		return thriftConnector;
	}

	public MasterHandler getThriftHandler()
	{
		return thriftHandler;
	}

	public void setThriftHandler(MasterHandler thriftHandler)
	{
		this.thriftHandler = thriftHandler;
	}

	public void setConnecion(ProviderConnection thriftConnection)
	{
		this.thriftConnection = thriftConnection;
	}
	public ProviderConnection getThriftConnection()
	{
		return thriftConnection;
	}
	public synchronized void enabled()
	{
		if (disabled)
			disabled = false;
	}
	public synchronized void disabled()
	{
		if (!disabled)
			disabled = true;
	}

	public boolean isDisabled()
	{
		return disabled;
	}

}
