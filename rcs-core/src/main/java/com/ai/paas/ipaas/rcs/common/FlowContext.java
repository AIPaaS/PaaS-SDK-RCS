package com.ai.paas.ipaas.rcs.common;

import backtype.storm.task.TopologyContext;

/**
 * 流程上下文环境，是storm中TopologyContext的代理类
 * 
 * @author ygz
 *
 */
public class FlowContext {
	private TopologyContext mContext = null;

	public FlowContext(TopologyContext context) {
		mContext = context;
	}

	/**
	 * getThisComponentId
	 * 
	 * @return
	 */
	public String getThisComponentId() {
		return null;
	}

	/**
	 * getThisTaskId
	 * 
	 * @return
	 */
	public Integer getThisTaskId() {
		return 0;
	}

	public TopologyContext getFlowContext() {
		return this.mContext;
	}

}
