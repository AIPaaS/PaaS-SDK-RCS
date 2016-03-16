package com.ai.paas.ipaas.rcs.data;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

/**
 * 实时计算中心对外的数据格式, 是storm中Tuple的代理类
 * @author ygz
 *
 */
public class StreamData 
{
	private Tuple mTuple;
	
	/**
	 * 缺省构造函数
	 */
	public StreamData()
	{
		mTuple = null;	
	}
	
	/**
	 * 用Tuple 初始化的构造函数
	 */
	public StreamData(Tuple aTuple)
	{
		this.mTuple = aTuple;
	}
	
	/**
	 * 设置RSCData对象使用的Tuple对象
	 */
	public void setTuple(Tuple aTuple)
	{
		this.mTuple = aTuple;
	}

	/**
	 * 获取tuple
	 * @param aTuple
	 */
	public Tuple getTuple()
	{
		return this.mTuple ;
	}
	 
	public boolean contains(String arg0) { 
		return mTuple.contains(arg0);
	}
 
	public int fieldIndex(String arg0) { 
		return mTuple.fieldIndex(arg0);
	}
 
	public byte[] getBinary(int arg0) { 
		return mTuple.getBinary(arg0);
	}
 
	public byte[] getBinaryByField(String arg0) { 
		return mTuple.getBinaryByField(arg0);
	}
 
	public Boolean getBoolean(int arg0) { 
		return mTuple.getBoolean(arg0);
	}
 
	public Boolean getBooleanByField(String arg0) { 
		return mTuple.getBooleanByField(arg0);
	}
 
	public Byte getByte(int arg0) { 
		return mTuple.getByte(arg0);
	}
 
	public Byte getByteByField(String arg0) { 
		return mTuple.getByteByField(arg0);
	}
 
	public Double getDouble(int arg0) { 
		return mTuple.getDouble(arg0);
	}
 
	public Double getDoubleByField(String arg0) { 
		return mTuple.getDoubleByField(arg0) ;
	}
 
	public Fields getFields() { 
		return mTuple.getFields();
	}
 
	public Float getFloat(int arg0) { 
		return mTuple.getFloat(arg0);
	}
 
	public Float getFloatByField(String arg0) { 
		return mTuple.getFloatByField(arg0);
	}
 
	public Integer getInteger(int arg0) { 
		return mTuple.getInteger(arg0);
	}
 
	public Integer getIntegerByField(String arg0) { 
		return mTuple.getIntegerByField(arg0);
	}
 
	public Long getLong(int arg0) { 
		return mTuple.getLong(arg0);
	}
 
	public Long getLongByField(String arg0) { 
		return mTuple.getLongByField(arg0);
	}
 
	public Short getShort(int arg0) { 
		return mTuple.getShort(arg0);
	}
 
	public Short getShortByField(String arg0) { 
		return mTuple.getShortByField(arg0);
	}

	public String getString(int arg0) 
	{
		return mTuple.getString(arg0); 
	}

	public String getStringByField(String aFieldName) 
	{
		return mTuple.getStringByField(aFieldName);
	}
 
	public Object getValue(int arg0) 
	{ 
		return mTuple.getValue(arg0);
	}
 
	public Object getValueByField(String arg0) 
	{ 
		return mTuple.getValueByField(arg0);
	}
 
	public List<Object> getValues() 
	{ 
		return mTuple.getValues();
	}
 
	public List<Object> select(Fields arg0) 
	{ 
		return mTuple.select(arg0);
	}
 
	public int size() 
	{ 
		return mTuple.size();
	}
 
	public MessageId getMessageId() { 
		return mTuple.getMessageId();
	}
 
	public String getSourceComponent() { 
		return mTuple.getSourceComponent();
	}
 
	public GlobalStreamId getSourceGlobalStreamid() { 
		return mTuple.getSourceGlobalStreamid() ;
	}
 
	public String getSourceStreamId() { 
		return mTuple.getSourceStreamId();
	}
 
	public int getSourceTask() { 
		return mTuple.getSourceTask();
	}

}
