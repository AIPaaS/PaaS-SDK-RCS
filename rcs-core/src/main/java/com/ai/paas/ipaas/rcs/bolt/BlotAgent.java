package com.ai.paas.ipaas.rcs.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.rcs.common.FlowContext;
import com.ai.paas.ipaas.rcs.common.ProcessorCollector;
import com.ai.paas.ipaas.rcs.data.StreamData;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 是storm中BaseRichBolt的代理类
 * 
 * @author ygz
 *
 */
public class BlotAgent extends BaseRichBolt implements SetValueListener {
	/**
	 * 序列号
	 */
	private static final long serialVersionUID = -6056742479132353101L;
	private StreamData mRCSData;
	private OutputCollector collector;
	private List<String> mFields;
	private String mProcessorName;
	private Logger LOG = null;
	private String logID;

	/**
	 * 外部的业务逻辑处理对象
	 */
	private Processor mProcessor;

	/**
	 * 缺省构造函数
	 */
	public BlotAgent() {
		mProcessorName = null;
		mProcessor = null;
	}

	/**
	 * 缺省构造函数
	 * 
	 * @param aProcessorClassName
	 *            输入类的名称
	 * @param fields
	 *            该输入类声明的输出字段列表
	 */
	@SuppressWarnings("unchecked")
	public BlotAgent(String aProcessorClassName, List<String> fields) {
		this.mProcessorName = aProcessorClassName;
		this.mFields = fields;

		// 通过java反射，创建输入类的实例
		Class<Processor> aClass = null;
		try {
			aClass = (Class<Processor>) Class.forName(mProcessorName);
		} catch (ClassNotFoundException e) {
			LOG.error("in prepare() -ClassNotFoundException: " + e.getMessage() + "-" + e.getCause());

			return;
		}

		try {
			mProcessor = (Processor) aClass.newInstance();
			//mProcessor.buildLogger(buildLogger(logID));
		} catch (InstantiationException | IllegalAccessException e) {
			LOG.error("in prepare() -InstantiationException | IllegalAccessException: " + e.getMessage() + "-" + e.getCause());
		}
	}

	/**
	 * 设置业务处理对象
	 */
	public void setProcessor(Processor aProcessor) {
		this.mProcessor = aProcessor;
	}

	@Override
	public void execute(Tuple aTuple) {
		// 把storm的Tuple类型转换为RCS的RCSData类型
		mRCSData.setTuple(aTuple);

		// 调用业务处理对象的执行函数
		mProcessor.execute(mRCSData);
		if (mProcessor.isAcked()) {
			this.collector.ack(aTuple);
		} else if (this.mProcessor.isFailed()) {
			this.collector.fail(aTuple);
		}
	}

	@Override
	public void valueBeSeted(List<Object> values) {
		if (CollectionUtils.isNotEmpty(values)) {
			List<Tuple> tuples = new ArrayList<>();
			tuples.add(mRCSData.getTuple());
			this.collector.emit(mRCSData.getTuple(), values);
		}
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector aCollector) {
		mRCSData = new StreamData();

		this.collector = aCollector;
		ProcessorCollector aRCSProcessorCollector = new ProcessorCollector(aCollector);

		FlowContext aRCSContext = new FlowContext(arg1);

		mProcessor.prepare(arg0, aRCSContext, aRCSProcessorCollector);
		mProcessor.setSetValueListener(this);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer aDeclarer) {
		// 只有用户设置了输出字段，才设置到拓扑中
		if (mProcessor.getOutFields() != null) {
			aDeclarer.declare(new Fields(mProcessor.getOutFields().toList()));
		}
	}

	@Override
	public void cleanup() {
		mProcessor.cleanup();
	}

	/**
	 * 实现该方法可以可以操作日志查看业务 使用为：this.LOG = LoggerFactory.getLogger(logId);
	 * 
	 * @param logId
	 *            分配给环境中日志对象的id序列号，由该序列号产生的日志对象是相同的，根据该logid使用工厂类创建日志对象
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
