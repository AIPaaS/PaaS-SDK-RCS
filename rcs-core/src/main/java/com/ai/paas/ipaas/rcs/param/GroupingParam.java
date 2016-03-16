package com.ai.paas.ipaas.rcs.param;

import java.util.ArrayList;

public class GroupingParam {
	private String previousId;
	private String groupType;
	private ArrayList<String> grpingFieldList;

	public GroupingParam() {
	}

	public GroupingParam(String previousId, String groupType) {
		this.previousId = previousId;
		this.groupType = groupType;
	}

	public String getPreviousId() {
		return previousId;
	}

	public void setPreviousId(String previousId) {
		this.previousId = previousId;
	}

	public ArrayList<String> getGrpingFieldList() {
		return grpingFieldList;
	}

	public void setGrpingFieldList(ArrayList<String> grpingFieldList) {
		this.grpingFieldList = grpingFieldList;
	}

	public String getGroupType() {
		return groupType;
	}

	public void setGroupType(String groupType) {
		this.groupType = groupType;
	}

}
