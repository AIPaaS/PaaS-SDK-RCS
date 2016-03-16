package com.ai.paas.ipaas.rcs.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.rcs.bolt.BlotAgent;
import com.ai.paas.ipaas.rcs.bolt.Processor;
import com.ai.paas.ipaas.rcs.param.BoltParam;
import com.ai.paas.ipaas.rcs.param.GroupingParam;
import com.ai.paas.ipaas.rcs.param.SpoutParam;
import com.ai.paas.ipaas.rcs.spout.Input;
import com.ai.paas.ipaas.rcs.spout.KafkaInput;
//import com.ai.paas.ipaas.rcs.spout.MDSInput;
import com.ai.paas.ipaas.rcs.spout.SpoutAgent;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

/**
 * 模块类，是storm中TopologyBuilder类的代理类，这里设置spout和bolt模块的相关信息
 * 
 * @author ygz
 *
 */
public class Module {
	protected Map<String, SpoutParam> mSpoutInfo = null;// 保存用户输入的输入类信息

	protected Map<String, BoltParam> mBoltInfo = null;// 保存用户输入的处理类的信息

	protected TopologyBuilder mBuilder = null;

	private Logger LOG = null;

	/**
	 * default constructor
	 */
	public Module(String aFlowID) {
		LOG = LoggerFactory.getLogger(aFlowID);

		mBuilder = new TopologyBuilder();

		mSpoutInfo = new HashMap<String, SpoutParam>();
		mBoltInfo = new HashMap<String, BoltParam>();

	}

	/**
	 * 设置输入类, 在拓扑发布到集群之前，不能创建对象，所以设置输入类的对象函数先不使用 或者实现对象的序列化，也可以使用这个接口
	 */
	public void setInput(String id, Input aInput) {
		SpoutAgent aSpout = new SpoutAgent();
		aSpout.setInput(aInput);

		mBuilder.setSpout(id, aSpout);
	}

	public void setKafkaInput(Map conf) {
		int parallelNum = Integer.parseInt((String) conf.get(KafkaInput.KAFKA_INPUT_PARALLEL_NUM));
		parallelNum = (parallelNum == 0) ? 1 : parallelNum;
		this.mBuilder.setSpout(KafkaInput.INPUT_NAME, new KafkaInput(conf).getKafkaSpout(), Integer.valueOf(parallelNum));
	}

	/**
	 * 通过输入类的名称设置输入类，该类的对象在真正使用的时候，才会被创建
	 * 
	 * @param id
	 *            - 在拓扑中的标识
	 * @param className
	 *            - 输入类的类名称
	 * @param outFields
	 *            - 输入类声明的输出字段列表
	 */
	public void setInput(String id, String className, List<String> outFields) {
		SpoutParam aParam = null;

		// 先查看是否已经存在该输入类
		if (mSpoutInfo.containsKey(id)) {
			aParam = (SpoutParam) mSpoutInfo.get(id);
		} else {
			// 保存输入类的信息
			aParam = new SpoutParam();

			aParam.setSpoutName(id);
			aParam.setOutFields(outFields);
			aParam.setSpoutClassName(className);
			aParam.setParallelNum(1);

			mSpoutInfo.put(id, aParam);
		}
	}

	/**
	 * 通过输入类的名称设置输入类，该类的对象在真正使用的时候，才会被创建
	 * 
	 * @param id
	 *            - 在拓扑中的标识
	 * @param className
	 *            - 输入类的类名称
	 * @param outFields
	 *            - 输入类声明的输出字段列表
	 */
	public void setInput(String id, String className, List<String> outFields, int parallelNumber) {
		SpoutParam aParam = null;

		// 先查看是否已经存在该输入类
		if (mSpoutInfo.containsKey(id)) {
			aParam = (SpoutParam) mSpoutInfo.get(id);

			aParam.setParallelNum(parallelNumber);
		} else {
			// 保存输入类的信息
			aParam = new SpoutParam();

			aParam.setSpoutName(id);
			aParam.setOutFields(outFields);
			aParam.setSpoutClassName(className);
			aParam.setParallelNum(parallelNumber);

			mSpoutInfo.put(id, aParam);
		}
	}

	/**
	 * 通过Class类参数，设置输入类，该类的对象在真正使用的时候，才会被创建
	 * 
	 * @param id
	 *            - 在拓扑中的标识
	 * @param aClass
	 *            - 输入类
	 * @param outFields
	 *            - 输入类声明的输出字段列表
	 */
	public void setInput(String id, @SuppressWarnings("rawtypes") Class aClass, List<String> outFields) {
		SpoutParam aParam = null;

		// 先查看是否已经存在该输入类
		if (mSpoutInfo.containsKey(id)) {
			aParam = (SpoutParam) mSpoutInfo.get(id);
			// aParam.setOutFields(outFields);
		} else {
			// 保存输入类的信息
			aParam = new SpoutParam();

			aParam.setSpoutName(id);
			aParam.setOutFields(outFields);
			aParam.setSpoutClassName(aClass.getName());
			aParam.setParallelNum(1);

			mSpoutInfo.put(id, aParam);
		}
	}

	/**
	 * 获取拓扑对象
	 */
	public TopologyBuilder getBuilder() {
		return mBuilder;
	}

	/**
	 * 设置处理类，该方法暂不使用，因为，storm在拓扑发布之前，对创建的对象要求能序列化
	 */
	public void setProcessor(String id, Processor aProcessor, String aGroupingType, String PreviousId) {
		BlotAgent aRCSBlot = new BlotAgent();
		aRCSBlot.setProcessor(aProcessor);

		if (aGroupingType.equals("shuffle")) {
			mBuilder.setBolt(id, aRCSBlot).shuffleGrouping(PreviousId);
		} else if (aGroupingType.equals("fields")) {
			// mBuilder.setBolt(entry.getKey(), aRCSBlot,
			// entry.getValue().getParallelNum()).fieldsGrouping(entry.getValue().getPreviousId(),
			// new
			// backtype.storm.tuple.Fields(entry.getValue().getGrpingFieldList()));
		} else {
			System.out.println(this.getClass().getName() + " - Wrong group type . [" + aGroupingType + "]\n");
			LOG.error("Wrong group type . [" + aGroupingType + "] valid one is shuffle or fields now.\n");
		}
	}

	/**
	 * 通过处理类名称设置bolt
	 * 
	 * @param id
	 *            - 处理类在拓扑中的标识，也是对应的bolt的标识
	 * @param className
	 *            - 处理类的名称
	 * @param outFields
	 *            - 处理类声明的输出字段列表
	 * @param aGroupingType
	 *            - 数据分组的方法,default is shuffleGrouping
	 * @param PreviousId
	 *            - 在拓扑中其前驱的标识
	 */
	public void setProcessor(String id, String className, String aGroupingType, String PreviousId) {
		BoltParam aParam = null;

		// 先查看是否已经存在该输入类
		if (mBoltInfo.containsKey(id)) {
			aParam = (BoltParam) mBoltInfo.get(id);

			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
		} else {
			// 保存处理类的信息
			aParam = new BoltParam();

			aParam.setBoltName(id);
			aParam.setBoltClassName(className);
			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
			aParam.setParallelNum(1);

			mBoltInfo.put(id, aParam);
		}
	}

	/**
	 * 通过处理类类参数设置bolt
	 * 
	 * @param id
	 *            - 处理类在拓扑中的标识，也是对应的bolt的标识
	 * @param aClass
	 *            - 用户定义的处理类
	 * @param outFields
	 *            - 处理类声明的输出字段列表
	 * @param aGroupingType
	 *            - 数据分组的方法,default is shuffleGrouping
	 * @param PreviousId
	 *            - 在拓扑中其前驱的标识
	 */
	public void setProcessor(String id, @SuppressWarnings("rawtypes") Class aClass, int parallelNum, String aGroupingType, String PreviousId) {
		BoltParam aParam = null;

		// 先查看是否已经存在该输入类
		if (mBoltInfo.containsKey(id)) {
			System.out.print(this.getClass().getName() + "containsKey [" + id + "]\n");

			aParam = (BoltParam) mBoltInfo.get(id);

			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
			 aParam.setParallelNum(parallelNum);
		} else {
			System.out.print(this.getClass().getName() + "not containsKey [" + id + "]\n");

			// 保存处理类的信息
			aParam = new BoltParam();

			aParam.setBoltName(id);
			aParam.setBoltClassName(aClass.getName());
			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
			aParam.setParallelNum(parallelNum);

			mBoltInfo.put(id, aParam);
		}
	}

	/**
	 * 设置处理类
	 * 
	 * @param id
	 *            - 处理类在拓扑中的标识，也是对应的bolt的标识
	 * @param className
	 *            - 处理类的名称
	 * @param parallelNum
	 *            - 处理类的并发度，也是对应的bolt的并发度
	 * @param outFields
	 *            - 处理类声明的输出字段列表
	 * @param aGroupingType
	 *            - 数据分组的方法, default is fieldsGrouping
	 * @param aGrpingFieldList
	 *            - 数据分组的字段，当按字段分组的时候，需要填写
	 * @param PreviousId
	 *            - 在拓扑中其前驱的标识
	 */
	public void setProcessor(String id, String className, int parallelNum, List<String> outFields, String aGroupingType, ArrayList<String> aGrpingFieldList, String PreviousId) {
		BoltParam aParam = null;

		// 先查看是否已经存在该输入类
		if (mBoltInfo.containsKey(id)) {
			System.out.print(this.getClass().getName() + "containsKey [" + id + "]\n");

			aParam = (BoltParam) mBoltInfo.get(id);

			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
			aParam.setParallelNum(parallelNum);
		} else {
			System.out.print(this.getClass().getName() + "not containsKey [" + id + "]\n");

			// 保存处理类的信息
			aParam = new BoltParam();

			aParam.setBoltName(id);
			aParam.setBoltClassName(className);
			aParam.setOutFields(outFields);
			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
			aParam.setParallelNum(parallelNum);
			aParam.setGrpingFieldList(aGrpingFieldList);

			mBoltInfo.put(id, aParam);
		}
	}

	/**
	 * 设置处理类，通过Class类参数
	 * 
	 * @param id
	 *            - 处理类在拓扑中的标识，也是对应的bolt的标识
	 * @param aClass
	 *            - 处理类
	 * @param parallelNum
	 *            - 处理类的并发度，也是对应的bolt的并发度
	 * @param outFields
	 *            - 处理类声明的输出字段列表
	 * @param aGroupingType
	 *            - 数据分组的方法, default is fieldsGrouping
	 * @param aGrpingFieldList
	 *            - 数据分组的字段，当按字段分组的时候，需要填写
	 * @param PreviousId
	 *            - 在拓扑中其前驱的标识
	 */
	public void setProcessor(String id, @SuppressWarnings("rawtypes") Class aClass, int parallelNum, List<String> outFields, String aGroupingType, ArrayList<String> aGrpingFieldList,
			String PreviousId) {
		BoltParam aParam = null;

		// 先查看是否已经存在该输入类
		if (mBoltInfo.containsKey(id)) {
			System.out.print(this.getClass().getName() + "containsKey [" + id + "]\n");

			aParam = (BoltParam) mBoltInfo.get(id);

			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
			aParam.setParallelNum(parallelNum);
		} else {
			System.out.print(this.getClass().getName() + "not containsKey [" + id + "]\n");

			// 保存处理类的信息
			aParam = new BoltParam();

			aParam.setBoltName(id);
			aParam.setBoltClassName(aClass.getName());
			aParam.setOutFields(outFields);
			aParam.setGroupingTypes(aGroupingType);
			aParam.setPreviousId(PreviousId);
			aParam.setParallelNum(parallelNum);
			aParam.setGrpingFieldList(aGrpingFieldList);

			mBoltInfo.put(id, aParam);
		}
	}

	/**
	 * 获取用户定义的输入类input的信息, 以map的形式，map的key是spout的标识
	 * 
	 * @return
	 */
	public Map<String, SpoutParam> getSpoutInfo() {
		return mSpoutInfo;
	}

	/**
	 * 获取用户定义的输入类input的信息, 以list的形式
	 * 
	 * @return
	 */
	public List<SpoutParam> getSpoutList() {
		List<SpoutParam> spoutParamList = new ArrayList<SpoutParam>();

		for (Map.Entry<String, SpoutParam> entry : mSpoutInfo.entrySet()) {
			SpoutParam aSpoutParam = new SpoutParam();

			aSpoutParam.setSpoutName(entry.getKey());
			aSpoutParam.setSpoutClassName(entry.getValue().getSpoutClassName());
			aSpoutParam.setParallelNum(entry.getValue().getParallelNum());

			spoutParamList.add(aSpoutParam); // 往list加入bolt1
		}

		return spoutParamList; // 返回list
	}

	/**
	 * 设置用户定义的输入类input的信息
	 * 
	 * @param mSpoutInfo
	 */
	public void setSpoutInfo(Map<String, SpoutParam> mSpoutInfo) {
		this.mSpoutInfo = mSpoutInfo;
	}

	/**
	 * 获取bolt引用的用户定义的processor的信息，以map的形式输出 map的key是bolt的标识。
	 * 
	 * @return
	 */
	public Map<String, BoltParam> getBoltInfo() {
		return mBoltInfo;
	}

	/**
	 * 获取bolt引用的用户定义的processor的信息, 以对象列表的形式输出
	 * 
	 * @return
	 */
	public List<BoltParam> getBoltList() {
		List<BoltParam> boltParamList = new ArrayList<BoltParam>();

		for (Map.Entry<String, BoltParam> entry : mBoltInfo.entrySet()) {
			BoltParam aBoltParam = new BoltParam();

			aBoltParam.setBoltName(entry.getKey());
			aBoltParam.setBoltClassName(entry.getValue().getBoltClassName());
			aBoltParam.setGroupingTypes(entry.getValue().getGroupingTypes());
			aBoltParam.setParallelNum(entry.getValue().getParallelNum());
			aBoltParam.setPreviousId(entry.getValue().getPreviousId());

			boltParamList.add(aBoltParam); // 往list加入bolt1
		}

		return boltParamList; // 返回list

	}

	/**
	 * build 把输入类和处理类设置到拓扑中，构建拓扑流程图
	 */
	public void build(String aFlowID) {
		// 设置spout
		for (Map.Entry<String, SpoutParam> entry : mSpoutInfo.entrySet()) {
			System.out.print(this.getClass().getName() + " - spout id = [" + entry.getValue().getSpoutName() + "]\n");

			// 创建输入类的spout
			SpoutAgent aSpout = new SpoutAgent(entry.getValue().getSpoutClassName(), entry.getValue().getOutFields());
			aSpout.setLogID(aFlowID);

			// 设置输入类的spout到拓扑中
			mBuilder.setSpout(entry.getKey(), aSpout, entry.getValue().getParallelNum());
		}

		// 设置bolt
		for (Map.Entry<String, BoltParam> entry : mBoltInfo.entrySet()) {
			String boltName = entry.getKey();
			BoltParam boltParam = entry.getValue();
			System.out.print(this.getClass().getName() + " - bolt id = [" + boltParam.getBoltName() + "]\n");

			BlotAgent aRCSBlot = new BlotAgent(boltParam.getBoltClassName(), boltParam.getOutFields());
			aRCSBlot.setLogID(aFlowID);

			if (boltParam.getGroupingTypes().equals("shuffle")) {
				mBuilder.setBolt(boltName, aRCSBlot, boltParam.getParallelNum()).shuffleGrouping(boltParam.getPreviousId());
			} else if (boltParam.getGroupingTypes().equals("fields")) {
				mBuilder.setBolt(boltName, aRCSBlot, boltParam.getParallelNum()).fieldsGrouping(boltParam.getPreviousId(), new backtype.storm.tuple.Fields(boltParam.getGrpingFieldList()));
			} else {
				System.out.println(this.getClass().getName() + " - Wrong group type . [" + boltParam.getGroupingTypes() + "]\n");
				LOG.error("Wrong group type . [" + boltParam.getGroupingTypes() + "] the valid one is shuffle or fields now.\n");
			}
		}
	}

	public void build2(String aFlowID) {
		// 设置spout
		for (Map.Entry<String, SpoutParam> entry : mSpoutInfo.entrySet()) {
			System.out.print(this.getClass().getName() + " - spout id = [" + entry.getValue().getSpoutName() + "]\n");

			// 创建输入类的spout
			SpoutAgent aSpout = new SpoutAgent(entry.getValue().getSpoutClassName(), entry.getValue().getOutFields());
			aSpout.setLogID(aFlowID);

			// 设置输入类的spout到拓扑中
			mBuilder.setSpout(entry.getKey(), aSpout, entry.getValue().getParallelNum());
		}

		// 设置bolt
		for (Map.Entry<String, BoltParam> entry : mBoltInfo.entrySet()) {
			String boltName = entry.getKey();
			BoltParam boltParam = entry.getValue();
			System.out.print(this.getClass().getName() + " - bolt id = [" + boltParam.getBoltName() + "]\n");

			BlotAgent aRCSBlot = new BlotAgent(boltParam.getBoltClassName(), boltParam.getOutFields());
			aRCSBlot.setLogID(aFlowID);
			BoltDeclarer boltDeclarer = mBuilder.setBolt(boltName, aRCSBlot, boltParam.getParallelNum());
			for (GroupingParam groupingParam : boltParam.getGroupingParams()) {
				if (boltParam.getGroupingTypes().equals("shuffle")) {
					boltDeclarer.shuffleGrouping(groupingParam.getPreviousId());
				} else if (boltParam.getGroupingTypes().equals("fields")) {
					boltDeclarer.fieldsGrouping(groupingParam.getPreviousId(), new backtype.storm.tuple.Fields(groupingParam.getGrpingFieldList()));
				} else {
					System.out.println(this.getClass().getName() + " - Wrong group type . [" + boltParam.getGroupingTypes() + "]\n");
					LOG.error("Wrong group type . [" + boltParam.getGroupingTypes() + "] the valid one is shuffle or fields now.\n");
				}
			}
		}
	}

	/**
	 * 设置用户定义的processor的信息
	 * 
	 * @param mBoltInfo
	 */
	public void setBoltInfo(Map<String, BoltParam> mBoltInfo) {
		this.mBoltInfo = mBoltInfo;
	}

}
