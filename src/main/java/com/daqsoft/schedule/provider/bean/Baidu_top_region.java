package com.daqsoft.schedule.provider.bean;

import org.springframework.data.annotation.Id;

/**
 * 百度 top 搜索风云榜,地区代码实体类
 * 
 * @author ShawnShoper
 *
 */
public class Baidu_top_region
{
	private String name;
	private String code;
	private String fcode;

	public String getFcode()
	{
		return fcode;
	}
	public void setFcode(String fcode)
	{
		this.fcode = fcode;
	}
	@Id
	private String id;
	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	public String getCode()
	{
		return code;
	}
	public void setCode(String code)
	{
		this.code = code;
	}
	public String getId()
	{
		return id;
	}
	public void setId(String id)
	{
		this.id = id;
	}

}
