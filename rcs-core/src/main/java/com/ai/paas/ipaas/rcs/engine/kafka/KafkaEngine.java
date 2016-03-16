package com.ai.paas.ipaas.rcs.engine.kafka;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
//import kafka.consumer.Consumer;
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.consumer.Whitelist;
//import kafka.javaapi.consumer.ConsumerConnector;
//import kafka.message.MessageAndMetadata;

public class KafkaEngine
{
//  private ConsumerConnector consumerConnector;

  private KafkaEngine()
  {
    try
    {
      Properties properties = new Properties();
      properties.load(KafkaEngine.class.getClassLoader().getResourceAsStream("kafka.properties"));
//      ConsumerConfig consumerConfig = new ConsumerConfig(properties);
//      this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static KafkaEngine getInstance()
  {
//    return InstanceFactory.instance;
	  return null;
  }

  public List<String> getTopic(String topic) throws Exception 
  {
    List ret = new ArrayList();
//    List partitions = this.consumerConnector.createMessageStreamsByFilter(new Whitelist(topic));
//    if (partitions.size() == 0) {
//      return null;
//    }
//    
//    for (KafkaStream partition : partitions) 
//    {
//      ConsumerIterator iterator = partition.iterator();
//      while (iterator.hasNext()) {
//        MessageAndMetadata next = iterator.next();
//        String message = new String((byte[])next.message(), "utf-8");
//        ret.add(message);
//      }
//    }
    
    return ret;
  }

  public static void main(String[] args) throws Exception
  {
    System.out.println(getInstance().getTopic("xxx"));
  }

  private static class InstanceFactory
  {
//    private static KafkaEngine instance = new KafkaEngine(null);
  }
}