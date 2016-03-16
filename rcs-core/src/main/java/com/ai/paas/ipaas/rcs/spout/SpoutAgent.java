package com.ai.paas.ipaas.rcs.spout;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.rcs.common.FlowContext;
import com.ai.paas.ipaas.rcs.common.InputCollector;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * 平台的spout实现类,调用Input类 是storm中IRichSpout的代理类
 */
public class SpoutAgent implements IRichSpout {
	/**
	 * 类序列号
	 */
	private static final long serialVersionUID = -794735067272607501L;
	private SpoutOutputCollector mCollector;
	private TopologyContext mContext;
	private Input mInput = null;
	private List<String> mFields = null;
	private String mInputName; // Class name of Input, defined by the user.
	private Logger LOG = null;
	private String logID;

	/**
	 * 缺省构造函数
	 */
	public SpoutAgent() {
		mInput = null;
		mContext = null;
		mInputName = null;
	}

	/**
	 * 缺省构造函数
	 */
	public SpoutAgent(String aInputClassName, List<String> fields) {
		mInput = null;
		mContext = null;
		this.mFields = fields;
		this.mInputName = aInputClassName;
	}

	/**
	 * 设置输入对象
	 */
	public void setInput(Input aInput) {
		this.mInput = aInput;
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.mContext = context;
		InputCollector aCollector = new InputCollector(collector);
		FlowContext aFlowContext = new FlowContext(context);
		// 通过java反射，创建输入类的实例
		Class<Input> aClass = null;
		try {
			aClass = (Class<Input>) Class.forName(mInputName);
		} catch (ClassNotFoundException e) {
			LOG.error("ClassNotFoundException: " + e.getMessage() + "-" + e.getCause());

			return;
		}

		try {
			mInput = (Input) aClass.newInstance();
			mInput.buildLogger(buildLogger(logID));
		} catch (InstantiationException | IllegalAccessException e) {
			LOG.error("InstantiationException| IllegalAccessException: " + e.getMessage() + "-" + e.getCause());

			return;
		}

		mInput.open(conf, aFlowContext, aCollector);

		this.mCollector = collector;
	}

	/**
	 * 
	 */
	@Override
	public void close() {

	}

	/**
	 * 
	 */
	@Override
	public void activate() {

	}

	/**
	 * 
	 */
	@Override
	public void deactivate() {

	}

	/**
	 * 获取下一个数据
	 */
	@Override
	public void nextTuple() {
		String aValue = mInput.getNext();
		this.mCollector.emit(this.mInput.values(), this.mInput.messageId());
	}

	/**
	 * 数据处理成功之后，需要做的处理逻辑
	 */
	@Override
	public void ack(Object msgId) {
		mInput.ack(msgId);
	}

	/**
	 * 数据处理失败之后，需要做的处理逻辑
	 */
	@Override
	public void fail(Object msgId) {
		mInput.fail(msgId);
	}

	/**
	 * 把用户的声明传入Storm
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields aFields = new Fields(mFields);
		declarer.declare(aFields);
	}

	/**
	 * 
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**
	 * 设置日志标识
	 * 
	 * @param logId
	 */
	public Logger buildLogger(String logId) {
		return LoggerFactory.getLogger(logId);
	}

	/**
	 * 设置拓扑ID
	 * 
	 * @param logId
	 */
	public void setLogID(String logID) {
		this.logID = logID;
	}

}
