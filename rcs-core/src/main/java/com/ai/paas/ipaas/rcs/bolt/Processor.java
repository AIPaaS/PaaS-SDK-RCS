package com.ai.paas.ipaas.rcs.bolt;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.ai.paas.ipaas.rcs.common.Fields;
import com.ai.paas.ipaas.rcs.common.FlowContext;
import com.ai.paas.ipaas.rcs.common.ProcessorCollector;
import com.ai.paas.ipaas.rcs.data.StreamData;

/**
 * 业务处理的抽象类，由使用者继承实现其中的抽象方法，该类的对象由对应的Blot调用
 * 
 * @author ygz
 *
 */
public abstract class Processor implements Serializable{
	protected Logger LOG = null;
	private boolean acked = true;
	private boolean failed = false;
	private SetValueListener setValueListener;

	/**
	 * 缺省构造函数
	 */
	public Processor() {
	}

	protected void ack() {
		this.acked = true;
		this.failed = false;
	}

	protected void failed() {
		this.failed = true;
		this.acked = false;
	}

	protected void setValues(List<Object> values) {
		setValueListener.valueBeSeted(values);
	}

	public boolean isAcked() {
		return this.acked;
	}

	public boolean isFailed() {
		return this.failed;
	}

	public void setSetValueListener(SetValueListener setValueListener) {
		this.setValueListener = setValueListener;
	}

	/**
	 * 处理逻辑函数，需要子类实现
	 * 
	 * @param tuple
	 */
	public abstract void execute(StreamData adata);

	/**
	 * 声明输出字段列表，使用方法是： 使用者在实现的时候，直接调用：mOutFields.add("Name");
	 */
	// public abstract void declareOutputFields();

	/**
	 * 由用户在这里实现：声明输出的字段，供外部接口获取
	 * 
	 * @return
	 */
	public abstract Fields getOutFields();

	/**
	 * 由用户实现处理类执行之前需要做的准备工作
	 * 
	 * @param aConf
	 * @param aContext
	 * @param collector
	 */
	public abstract void prepare(@SuppressWarnings("rawtypes") Map aConf, FlowContext aContext, ProcessorCollector collector);

	/**
	 * 由用户实现处理类退出的时候，需要清理的工作，这里的清理不一定会被执行
	 */
	public abstract void cleanup();

	/**
	 * 实现该方法可以可以操作日志查看业务 使用为：this.LOG = LoggerFactory.getLogger(logId);
	 * 
	 * @param logId
	 *            分配给环境中日志对象的id序列号，由该序列号产生的日志对象是相同的，根据该logid使用工厂类创建日志对象
	 */
	public abstract void buildLogger(Logger LOG);
}
