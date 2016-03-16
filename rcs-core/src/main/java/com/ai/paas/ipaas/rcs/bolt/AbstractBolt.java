package com.ai.paas.ipaas.rcs.bolt;

import java.util.Map;

import org.slf4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * 所有Bolt的基类
 * @author weichuang
 *
 */
public abstract class AbstractBolt extends BaseRichBolt 
{
	private static final long serialVersionUID = 2640022318043545465L;
	protected Logger LOG = null;
	
	@Override
	public final void execute(Tuple tuple) {
		_execute(tuple);
	}

	public abstract void _execute(Tuple tuple);

	@Override
	public final void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topology, OutputCollector collector) {
		_prepare(map, topology, collector);
	}

	public abstract void _prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topology, OutputCollector collector);

	@Override
	public final void declareOutputFields(OutputFieldsDeclarer declarer) {
		_declareOutputFields(declarer);
	}

	public abstract void _declareOutputFields(OutputFieldsDeclarer declarer);
	
	/**
	 * 实现该方法可以可以操作日志查看业务
	 * 使用为：this.LOG = LoggerFactory.getLogger(logId);
	 * @param logId 分配给环境中日志对象的id序列号，由该序列号产生的日志对象是相同的，根据该logid使用工厂类创建日志对象
	 */
	public abstract void buildLogger(String logId);
}
