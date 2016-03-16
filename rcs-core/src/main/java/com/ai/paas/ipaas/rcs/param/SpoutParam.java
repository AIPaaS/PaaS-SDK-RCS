package com.ai.paas.ipaas.rcs.param;

import java.util.List;

/**
 * main 参数 spout
 * @author weichuang
 *
 */
public class SpoutParam 
{
	private String spoutName;
	private String spoutClassName;
	private int parallelNum;
	
	private List<String> outFields = null;

	/**
	 * default constructor
	 * @return
	 */
	public SpoutParam()
	{
		spoutName = "";
		spoutClassName  = "";
		outFields = null;
		parallelNum = 1;
	}
	
	/**
	 * 
	 * @return
	 */
	public List<String> getOutFields() {
		return outFields;
	}

	public void setOutFields(List<String> outFields) {
		this.outFields = outFields;
	}

	public String getSpoutName() 
	{
		return spoutName;
	}

	public void setSpoutName(String spoutName) 
	{
		this.spoutName = spoutName;
	}

	public String getSpoutClassName() 
	{
		return spoutClassName;
	}

	public void setSpoutClassName(String spoutClassName) 
	{
		this.spoutClassName = spoutClassName;
	}

	public int getParallelNum() 
	{
		return parallelNum;
	}

	public void setParallelNum(int threads) 
	{
		this.parallelNum = threads;
	}
}
