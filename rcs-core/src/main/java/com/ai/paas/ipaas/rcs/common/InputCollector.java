package com.ai.paas.ipaas.rcs.common;

import java.util.List;

import com.ai.paas.ipaas.rcs.data.StreamData;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * 输入集合类，是storm中SpoutOutputCollector的代理类 实现类似SpoutOutputCollector的emit方法，供用户调用
 * 
 * @author ygz
 *
 */
public class InputCollector {
	private SpoutOutputCollector mCollector = null;

	/**
	 * 缺省构造函数
	 */
	public InputCollector(SpoutOutputCollector aCollector) {
		mCollector = aCollector;
	}

	/**
	 * 提供给用户使用，往拓扑发送数据
	 * 
	 * @param aList
	 * @param messageId
	 * @return
	 */
	public List<Integer> send(List<Object> aList, Object messageId) {
		return mCollector.emit(Utils.DEFAULT_STREAM_ID, aList, messageId);
	}

	public SpoutOutputCollector getmCollector() {
		return mCollector;
	}

}
