package com.ai.paas.ipaas.rcs.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
 

import com.ai.paas.ipaas.rcs.data.StreamData;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * 处理类Processor，用到的输出集合类，是OutputCollector的代理类
 * @author ygz
 *
 */
public class ProcessorCollector 
{
	private OutputCollector mCollector = null;

	public ProcessorCollector(OutputCollector aCollector)
	{
		this.mCollector = aCollector;
		
	}
	
	/**
	 * 
	 * @param anchors
	 * @param tuple
	 * @return
	 */
    public List<Integer> emit(Collection<StreamData> anchors, List<Object> aData) 
    {
    	Collection<Tuple> aCollection = new ArrayList<Tuple>() ;
        return mCollector.emit(Utils.DEFAULT_STREAM_ID, aCollection, aData);
    }
    
    /**
     * 
     * @param input
     */
    public void ack(StreamData input) 
    {
    	Tuple aTuple = input.getTuple();
    	mCollector.ack(aTuple);
    }

    /**
     * 
     * @param input
     */
    public void fail(StreamData input) 
    {
    	Tuple aTuple = input.getTuple();
    	mCollector.fail(aTuple);
    }
}
