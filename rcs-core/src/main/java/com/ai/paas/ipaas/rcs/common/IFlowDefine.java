package com.ai.paas.ipaas.rcs.common;

import com.ai.paas.ipaas.rcs.param.FlowParam;

/**
 * 拓扑流程定义的接口
 * @author ygz
 *
 */
public interface IFlowDefine 
{
	
	/**
	 * 流程参数的定义
	 */
	
	
	/**
	 * 这里定义拓扑的流程的各个模块
	 * @param args
	 * @param builder
	 */
	public void define(String[] args, Module aModules, FlowParam aParams);
	
	/**
	 * 这里由用户定义流程的配置参数
	 * @param args
	 * @param aConfig
	 */
	public void configure(String[] args, FlowConfig aConfigs);

}
