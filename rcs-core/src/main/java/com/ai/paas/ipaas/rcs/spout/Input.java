package com.ai.paas.ipaas.rcs.spout;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.ai.paas.ipaas.rcs.common.Fields;
import com.ai.paas.ipaas.rcs.common.FlowContext;
import com.ai.paas.ipaas.rcs.common.InputCollector;

/**
 * 数据输入的抽象类，由用户继承，并实现其中的抽象方法，该类的对象由Spout调用
 * 
 * @author ygz
 *
 */
public abstract class Input {

	protected Logger LOG = null;
	private List<Object> values;
	private Object messageId;

	protected void setValues(List<Object> values) {
		this.values = values;
	}

	protected void setMessageId(Object messageId) {
		this.messageId = messageId;
	}

	public List<Object> values() {
		return this.values;
	}

	public Object messageId() {
		return this.messageId;
	}

	/**
	 * 缺省构造函数
	 */
	public Input() {

	}

	/**
	 * 获取数据的前的准备
	 */
	public abstract String open(@SuppressWarnings("rawtypes") Map conf, FlowContext aContext, InputCollector aCollector);

	/**
	 * 获取数据的逻辑
	 */
	public abstract String getNext();

	/**
	 * 获取数据的逻辑
	 */
	public abstract Fields getOutFields();

	/**
	 * 数据处理成功之后，实现处理过程的函数
	 */
	public abstract void ack(Object msgId);

	/**
	 * 数据处理失败之后，实现处理过程的函数
	 */
	public abstract void fail(Object msgId);

	/**
	 * 实现该方法可以可以操作日志查看业务 使用为：this.LOG = LoggerFactory.getLogger(logId);
	 * 
	 * @param logId
	 *            分配给环境中日志对象的id序列号，由该序列号产生的日志对象是相同的，根据该logid使用工厂类创建日志对象
	 */
	public abstract void buildLogger(Logger log);

}
