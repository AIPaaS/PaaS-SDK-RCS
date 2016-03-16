package com.ai.paas.ipaas.rcs.spout;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import com.ai.paas.ipaas.rcs.common.Fields;
import com.ai.paas.ipaas.rcs.common.FlowContext;
import com.ai.paas.ipaas.rcs.common.InputCollector;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaInput extends Input {
	public static final String INPUT_NAME = "kafka-input";
	public static final String KAFKA_INPUT_TOPIC = "kafka.input.topic";
	public static final String KAFKA_INPUT_ZK_SERVER = "kafka.input.zk.server";
	public static final String KAFKA_INPUT_ZK_PORT = "kafka.input.zk.port";
	public static final String KAFKA_INPUT_PARALLEL_NUM = "kafka.input.parallel.num";
	public static final String KAFKA_CONSUMER_ADDR = "kafka.consumer.addr";
	private KafkaSpout kafkaSpout;

	public KafkaInput(Map conf) {
		String topic = String.valueOf(conf.get("kafka.input.topic"));
		String zkServerStr = String.valueOf(conf.get("kafka.input.zk.server"));
		int zkPort = Integer.parseInt((String) conf.get("kafka.input.zk.port"));
		String zkAddr = String.valueOf(conf.get("kafka.consumer.addr"));
		BrokerHosts brokerHosts = new ZkHosts(zkAddr);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, "/kafkainput", UUID.randomUUID().toString());
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = false;

		spoutConf.zkServers = Arrays.asList(zkServerStr.split(",", -1));
		spoutConf.zkPort = Integer.valueOf(zkPort);
		this.kafkaSpout = new KafkaSpout(spoutConf);
	}

	public KafkaSpout getKafkaSpout() {
		return this.kafkaSpout;
	}

	public String open(Map conf, FlowContext aContext, InputCollector aCollector) {
		this.kafkaSpout.open(conf, aContext.getFlowContext(), aCollector.getmCollector());
		return null;
	}

	public String getNext() {
		this.kafkaSpout.nextTuple();
		return null;
	}

	public Fields getOutFields() {
		return new Fields(new String[] { INPUT_NAME });
	}

	public void ack(Object msgId) {
		this.kafkaSpout.ack(msgId);
	}

	public void fail(Object msgId) {
		this.kafkaSpout.fail(msgId);
	}

	public void buildLogger(Logger log) {
	}

}
