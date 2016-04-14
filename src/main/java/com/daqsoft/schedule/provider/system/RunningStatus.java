package com.daqsoft.schedule.provider.system;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.hyperic.sigar.SigarException;
import org.shoper.system.SystemUtil;
/**
 * provider running status..
 * 
 * @author ShawnShoper
 *
 */
public class RunningStatus
{
	static class Sum
	{
		private int sum;
		public int getSum()
		{
			return sum;
		}
		public void add(int num)
		{
			this.sum += num;
		}
	}
	// Fetch system cpu of the immutable
	static
	{
		// Get this machine cpu info;
		try
		{
			Sum totalMhz = new Sum();
			Arrays.stream(SystemUtil.getCpuInfoList()).parallel().forEach(cpu ->
			{
				totalMhz.add(cpu.getMhz());
			});
			cpuWeight = Integer.parseInt(totalMhz.getSum() / 1000 + "");

		} catch (SigarException e)
		{
			throw new ExceptionInInitializerError(e);
		}
	}
	public static void main(String[] args)
			throws NumberFormatException, SigarException
	{
		System.out.println(
				new BigDecimal(Double.valueOf(SystemUtil.getMemInfo().getFree())
						/ 1024 / 1024 / 1024 + "")
								.setScale(2, RoundingMode.HALF_EVEN)
								.add(new BigDecimal(RunningStatus.cpuWeight))
								.doubleValue());
	}
	public final static int cpuWeight;
	public static volatile int limitTask = 1;
	public static AtomicLong serviceTimes = new AtomicLong(0);
	public static AtomicLong failedTimes = new AtomicLong(0);
	public static AtomicLong successTimes = new AtomicLong(0);
	public static double getCpuIdlePercent()
			throws com.daqsoft.schedule.exception.SystemException
	{
		try
		{
			return BigDecimal.valueOf(SystemUtil.getCpuPercInfo().getIdle())
					.setScale(4, BigDecimal.ROUND_HALF_EVEN)
					.multiply(new BigDecimal(100)).doubleValue();
		} catch (SigarException e)
		{
			throw new com.daqsoft.schedule.exception.SystemException(e);
		}
	}

	public static double getMemUsedPercent()
			throws com.daqsoft.schedule.exception.SystemException
	{
		try
		{

			return BigDecimal.valueOf(SystemUtil.getMemInfo().getUsedPercent())
					.setScale(4, BigDecimal.ROUND_HALF_EVEN).doubleValue();
		} catch (SigarException e)
		{
			throw new com.daqsoft.schedule.exception.SystemException(e);
		}
	}
}
