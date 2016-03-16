package com.ai.paas.ipaas.rcs.param;

import java.util.ArrayList;
import java.util.List;

/**
 * main 参数 bolt
 * 
 * @author weichuang
 *
 */
public class BoltParam {
	private String boltName;
	private String boltClassName;
	private List<String> outFields = null;
	private String previousId;
	private int parallelNum;
	private ArrayList<String> grpingFieldList;
	private List<GroupingParam> groupingParams = new ArrayList<>();
	// private int threads;

	/**
	 * 多个，逗号分隔符
	 */
	private String groupingTypes;

	/**
	 * 多个，逗号分隔符
	 */
	private String groupingSpoutOrBolts;

	/**
	 * default constructor
	 */
	public BoltParam() {
		boltName = "";
		boltClassName = "";
		outFields = null;
		previousId = "";
		parallelNum = 1;
		grpingFieldList = null;
	}

	public void addGroupingParam(GroupingParam groupingParam) {
		this.groupingParams.add(groupingParam);
	}

	public List<GroupingParam> getGroupingParams() {
		return groupingParams;
	}

	public ArrayList<String> getGrpingFieldList() {
		return grpingFieldList;
	}

	public void setGrpingFieldList(ArrayList<String> grpingFieldList) {
		this.grpingFieldList = grpingFieldList;
	}

	public int getParallelNum() {
		return parallelNum;
	}

	public void setParallelNum(int parallelNum) {
		this.parallelNum = parallelNum;
	}

	public String getPreviousId() {
		return previousId;
	}

	public void setPreviousId(String previousId) {
		this.previousId = previousId;
	}

	public List<String> getOutFields() {
		return outFields;
	}

	public void setOutFields(List<String> outFields) {
		this.outFields = outFields;
	}

	public String getBoltName() {
		return boltName;
	}

	public void setBoltName(String boltName) {
		this.boltName = boltName;
	}

	public String getBoltClassName() {
		return boltClassName;
	}

	public void setBoltClassName(String boltClassName) {
		this.boltClassName = boltClassName;
	}

	public String getGroupingTypes() {
		return groupingTypes;
	}

	public void setGroupingTypes(String groupingTypes) {
		this.groupingTypes = groupingTypes;
	}

	public String getGroupingSpoutOrBolts() {
		return groupingSpoutOrBolts;
	}

	public void setGroupingSpoutOrBolts(String groupingSpoutOrBolts) {
		this.groupingSpoutOrBolts = groupingSpoutOrBolts;
	}

}
