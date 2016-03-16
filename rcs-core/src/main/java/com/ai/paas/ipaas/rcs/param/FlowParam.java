package com.ai.paas.ipaas.rcs.param;

import java.util.List;

/**
 * 流程参数，保存流程定义时设定的参数
 * @author weichuang
 *
 */
public class FlowParam 
{
	private String flowName;
	private int numWorkers;
	private List<SpoutParam> spouts;
	private List<BoltParam> bolts;

	/**
	 * 获取流程的名称
	 * @return
	 */
	public String getFlowName() 
	{
		return flowName;
	}

	/**
	 * 设置流程的名称
	 * @param flowName
	 */
	public void setFlowName(String flowName) 
	{
		this.flowName = flowName;
	}

	/**
	 * 获取工作进程的数量
	 * @return
	 */
	public int getNumWorkers() 
	{
		return numWorkers;
	}

	/**
	 * 设置工作进程的数量
	 * @param workerNumber
	 */
	public void setNumWorkers(int numWorkers) 
	{
		this.numWorkers = numWorkers;
	}

	public List<SpoutParam> getSpouts() 
	{
		return spouts;
	}

	public void setSpouts(List<SpoutParam> spouts) 
	{
		this.spouts = spouts;
	}

	public List<BoltParam> getBolts() 
	{
		return bolts;
	}

	public void setBolts(List<BoltParam> bolts) 
	{
		this.bolts = bolts;
	}
}
