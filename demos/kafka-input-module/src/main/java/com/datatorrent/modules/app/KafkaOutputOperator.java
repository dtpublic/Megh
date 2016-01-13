package com.datatorrent.modules.app;

import java.math.BigInteger;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.contrib.kafka.AbstractKafkaOutputOperator;

import kafka.producer.KeyedMessage;

public class KafkaOutputOperator extends AbstractKafkaOutputOperator<String, byte[]>
{
  public final transient DefaultInputPort<Integer> inputPort = new DefaultInputPort<Integer>() {
    @Override
    public void process(Integer tuple)
    {
      processTuple(tuple);
    }
  };

  protected void processTuple(Integer tuple)
  {
    getProducer().send(new KeyedMessage<String, byte[]>(getTopic(), BigInteger.valueOf(tuple).toByteArray()));
  }
}
