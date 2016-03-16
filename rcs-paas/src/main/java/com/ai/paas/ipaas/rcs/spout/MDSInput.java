package com.ai.paas.ipaas.rcs.spout;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import com.ai.paas.ipaas.ccs.zookeeper.ZKClient;
import com.ai.paas.ipaas.ccs.zookeeper.impl.ZKPoolFactory;
import com.ai.paas.ipaas.rcs.common.Fields;
import com.ai.paas.ipaas.rcs.common.FlowContext;
import com.ai.paas.ipaas.rcs.common.InputCollector;
import com.ai.paas.ipaas.uac.service.IUserClient;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.CiperUtil;
import com.google.gson.Gson;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class MDSInput extends Input {
	public static final String INPUT_NAME = "mds-input";
	public static final String PAAS_AUTH_ADDRESS = "paas.auth.address";
	public static final String PAAS_PID = "paas.pid";
	public static final String PAAS_MDS_INPUT_SERVICE_ID = "paas.mds.input.service.id";
	public static final String PAAS_MDS_INPUT_SERVICE_PWD = "paas.mds.input.service.pwd";
	public static final String PAAS_MDS_INPUT_TOPIC = "paas.mds.input.topic";
	public static final String KAFKASPOUT_ZK_SERVER = "kafkaspout.zk.server";
	public static final String KAFKASPOUT_ZK_PORT = "kafkaspout.zk.port";
	public static final String PAAS_MDS_INPUT_PARALLEL_NUM = "paas.mds.input.parallel.num";
	private static final String PAAS_MDS_CONSUMER_ADDR = "kafka.zookeeper.hosts";
	private KafkaSpout kafkaSpout;

	public MDSInput() {
	}

	public MDSInput(Map conf) {
		createKafkaSpout(conf);
	}

	public String open(Map conf, FlowContext aContext, InputCollector aCollector) {
		createKafkaSpout(conf);
		this.kafkaSpout.open(conf, aContext.getFlowContext(), aCollector.getmCollector());
		return null;
	}

	private void createKafkaSpout(Map conf) {
		String authAdress = (String) conf.get(PAAS_AUTH_ADDRESS);
		String pid = (String) conf.get(PAAS_PID);
		String mdsServiceId = (String) conf.get(PAAS_MDS_INPUT_SERVICE_ID);
		String mdsServicePwd = (String) conf.get(PAAS_MDS_INPUT_SERVICE_PWD);
		String topic = (String) conf.get(PAAS_MDS_INPUT_TOPIC);
		String zkServers = (String) conf.get(KAFKASPOUT_ZK_SERVER);
		int zkPort = Integer.parseInt((String) conf.get(KAFKASPOUT_ZK_PORT));
		AuthDescriptor ad = new AuthDescriptor(authAdress, pid, mdsServicePwd, mdsServiceId);
		IUserClient userClient = UserClientFactory.getUserClient();
		AuthResult result = userClient.auth(ad);
		try {
			ZKClient client = ZKPoolFactory.getZKPool(result.getConfigAddr(), result.getConfigUser(), CiperUtil.decrypt("1@3^$aGH;._|$!@#", result.getConfigPasswd()), ad.getServiceId())
					.getZkClient(result.getConfigAddr(), result.getConfigUser(), ad.getServiceId());

			String configValue = client.getNodeData(apendUserNodePath(ad, topic, result), false);
			Gson gson = new Gson();
			Map configMap = (Map) gson.fromJson(configValue, Map.class);
			String zkAddr = String.valueOf(configMap.get(PAAS_MDS_CONSUMER_ADDR));
			BrokerHosts brokerHosts = new ZkHosts(zkAddr);
			SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, "/kafkainput", UUID.randomUUID().toString());
			spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
			spoutConf.forceFromStart = false;
			spoutConf.zkServers = Arrays.asList(zkServers.split(",", -1));
			spoutConf.zkPort = Integer.valueOf(zkPort);
			// 强制从头开始消费
			spoutConf.forceFromStart = true;
			this.kafkaSpout = new KafkaSpout(spoutConf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String apendUserNodePath(AuthDescriptor ad, String topic, AuthResult result) {
		return "/com/ai/paas/ipaas/ccs/user/" + result.getUserId() + "/readonly" + "/MDS" + "/" + ad.getServiceId() + "/" + topic + "/" + "consumer";
	}

	public String getNext() {
		this.kafkaSpout.nextTuple();
		return null;
	}

	public Fields getOutFields() {
		return new Fields(new String[] { "mds_input" });
	}

	public void ack(Object msgId) {
		this.kafkaSpout.ack(msgId);
	}

	public void fail(Object msgId) {
		this.kafkaSpout.fail(msgId);
	}

	public void buildLogger(Logger log) {
		this.LOG = log;
	}

	public KafkaSpout getKafkaSpout() {
		return this.kafkaSpout;
	}
}
