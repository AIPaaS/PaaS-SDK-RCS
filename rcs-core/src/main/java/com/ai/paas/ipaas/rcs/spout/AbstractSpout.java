package com.ai.paas.ipaas.rcs.spout;

import java.util.Map;

import org.slf4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
/**
 * 所有Spout的基类
 * @author weichuang
 *
 */
public abstract class AbstractSpout extends BaseRichSpout {
	private static final long serialVersionUID = 5929634425330418998L;
	protected Logger LOG = null;
	
	@Override
	public final void nextTuple() {
		_nextTuple();
	}
	
	public abstract void _nextTuple();
	
	@Override
	public final void open(@SuppressWarnings("rawtypes") Map map, TopologyContext context, SpoutOutputCollector collector) {
		_open(map, context, collector);
	}
	
	public abstract void _open(@SuppressWarnings("rawtypes") Map map, TopologyContext context, SpoutOutputCollector collector);
	
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
