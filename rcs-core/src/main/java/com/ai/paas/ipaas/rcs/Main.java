package com.ai.paas.ipaas.rcs;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.ai.paas.ipaas.rcs.bolt.AbstractBolt;
import com.ai.paas.ipaas.rcs.param.BoltParam;
import com.ai.paas.ipaas.rcs.param.FlowParam;
import com.ai.paas.ipaas.rcs.param.SpoutParam;
import com.ai.paas.ipaas.rcs.spout.AbstractSpout;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class Main 
{
	public static final int NUM_WORKERS = 4;

	/**
	 * {'topologyName':'test','numWorkers':3,'spouts':[{'spoutName':'DemoSpout','spoutClassName':'com.ai.paas.ipaas.storm.spout.DemoSpout','threads':11}],'bolts':[{'boltName':'DemoBolt','boltClassName':'com.ai.paas.ipaas.storm.bolt.DemoBolt','threads':11,'groupingTypes':'','groupingSpoutOrBolts':'DemoSpout'}]}
	 */
	public static FlowParam validate(String[] args) 
	{
		if (args.length > 1) {
			throw new IllegalArgumentException("传入的参数有误，有且只能有一个");
		}
		
		FlowParam mainArgTopology;
		
	    try {
	      Gson gson = new Gson();
	      mainArgTopology = (FlowParam)gson.fromJson(args[0].replace("T__T_", "}").replace("_T__T", "{").replace("T__T", ",").replace("T_T", "'").replace("'", "\""), FlowParam.class);
	    } catch (JsonSyntaxException e) {
	      throw new IllegalArgumentException("传入的参数有误，示例为{'topologyName':'test','numWorkers':3,'spouts':[{'spoutName':'DemoSpout','spoutClassName':'com.ai.paas.ipaas.storm.spout.DemoSpout','threads':11}],'bolts':[{'boltName':'DemoBolt','boltClassName':'com.ai.paas.ipaas.storm.bolt.DemoBolt','threads':11,'groupingTypes':'','groupingSpoutOrBolts':'DemoSpout'}]}");
	    }
	    
		// //
		// MainArgTopology mainArgTopology = new MainArgTopology();
		// mainArgTopology.setNumWorkers(3);
		// mainArgTopology.setTopologyName("test");
		// //
		// List<MainArgSpout> spouts = new ArrayList<MainArgSpout>();
		// List<MainArgBolt> bolts = new ArrayList<MainArgBolt>();
		// MainArgSpout mainArgSpout = new MainArgSpout();
		// mainArgSpout.setSpoutClassName("com.ai.paas.ipaas.storm.spout.DemoSpout");
		// mainArgSpout.setSpoutName("DemoSpout");
		// mainArgSpout.setThreads(11);
		// spouts.add(mainArgSpout);
		// mainArgTopology.setSpouts(spouts);
		// //
		// MainArgBolt mainArgBolt = new MainArgBolt();
		// mainArgBolt.setBoltClassName("com.ai.paas.ipaas.storm.bolt.DemoBolt");
		// mainArgBolt.setBoltName("DemoBolt");
		// mainArgBolt.setGroupingSpoutOrBolts("DemoSpout");
		// mainArgBolt.setGroupingTypes("");
		// mainArgBolt.setThreads(11);
		// bolts.add(mainArgBolt);
		// mainArgTopology.setBolts(bolts);
		return mainArgTopology;
	}

	public static void defines(TopologyBuilder topologyBuilder, FlowParam mainArgTopology) throws Exception 
	{
		List<SpoutParam> spouts = mainArgTopology.getSpouts();
		
		//获取topology名
		String topologyName = mainArgTopology.getFlowName();
		for (SpoutParam mainArgSpout : spouts) 
		{
			@SuppressWarnings("rawtypes")
			Class class1 = Class.forName(mainArgSpout.getSpoutClassName());
			AbstractSpout spout = (AbstractSpout) class1.newInstance();
			
			//设置日志id
			spout.buildLogger(topologyName);
			topologyBuilder.setSpout(mainArgSpout.getSpoutName(), spout, mainArgSpout.getParallelNum());
		}
		
		//
		List<BoltParam> bolts = mainArgTopology.getBolts();
		for (BoltParam mainArgBolt : bolts) 
		{
			@SuppressWarnings("rawtypes")
			Class class1 = Class.forName(mainArgBolt.getBoltClassName());
			AbstractBolt bolt = (AbstractBolt) class1.newInstance();
			bolt.buildLogger(topologyName);
			topologyBuilder.setBolt(mainArgBolt.getBoltName(), bolt, mainArgBolt.getParallelNum()).shuffleGrouping(mainArgBolt.getGroupingSpoutOrBolts());
		}
	}

	public static void main(String[] args) throws Exception 
	{
		try {
			TopologyBuilder topologyBuilder = new TopologyBuilder();
			
			// storm jar xxx.jar com.ai.paas.ipaas.storm.Main {[spout]}
			//
			FlowParam mainArgTopology = validate(args);
			
			//
			defines(topologyBuilder, mainArgTopology);
			
			//
			Config conf = new Config();
			conf.setNumWorkers(mainArgTopology.getNumWorkers());
			
			//
			boolean isCluster = true;
			if (args != null && args.length > 0 && isCluster) 
			{
				conf.setNumWorkers(mainArgTopology.getNumWorkers());
				StormSubmitter.submitTopologyWithProgressBar(mainArgTopology.getFlowName(), conf, topologyBuilder.createTopology());
			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("test", conf, topologyBuilder.createTopology());
				Utils.sleep(1000000);
				cluster.killTopology("test");
				cluster.shutdown();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
