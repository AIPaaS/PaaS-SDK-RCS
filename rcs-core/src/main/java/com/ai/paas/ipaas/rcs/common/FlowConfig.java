package com.ai.paas.ipaas.rcs.common;

/** 
 * storm的Config类的代理类
 */ 

import backtype.storm.Config;
import backtype.storm.ConfigValidation;
import backtype.storm.serialization.IKryoDecorator;
import backtype.storm.serialization.IKryoFactory;

import com.esotericsoftware.kryo.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
public class FlowConfig
{
	
	private Config mConf = null;
	 
	public FlowConfig()
	{
		mConf = new Config();
	}
	
	/**
	 * 设置配置信息
	 */
	public void setConf(String id, String value)
	{
		System.out.print(this.getClass().getName() + " - id = [" + id + "] value = [" + value + "]\n");
		mConf.put(id, value);
	}
	
	/**
	 * 获取配置对象
	 */
	public Config getConf()
	{
		return mConf;
	}
	
	
	/**
	 * 设置是否调试
	 */
	public void setDebug(boolean isOn)
	{
		mConf.put(Config.TOPOLOGY_DEBUG, isOn);
	}
	
	/**
	 * 设置数据最大输入数
	 */
	public void setMaxInputPending(int number)
	{
		mConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, number);
	}
	
	/**
	 * setNumWorkers
	 * @param workers
	 */
    public void setNumWorkers(int workers) {
    	mConf.setNumWorkers(workers);
    }

}

