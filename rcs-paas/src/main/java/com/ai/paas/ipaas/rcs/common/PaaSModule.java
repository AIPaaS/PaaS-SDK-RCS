package com.ai.paas.ipaas.rcs.common;

import java.util.Map;

import com.ai.paas.ipaas.rcs.spout.MDSInput;

public class PaaSModule extends Module {

	public PaaSModule(String aFlowID) {
		super(aFlowID);
	}

	public void setMdsInput(Map conf) {
		int parallelNum = Integer.parseInt((String) conf.get(MDSInput.PAAS_MDS_INPUT_PARALLEL_NUM));
		parallelNum = (parallelNum == 0) ? 1 : parallelNum;
		super.mBuilder.setSpout(MDSInput.INPUT_NAME, new MDSInput(conf).getKafkaSpout(), Integer.valueOf(parallelNum));
	}
}
