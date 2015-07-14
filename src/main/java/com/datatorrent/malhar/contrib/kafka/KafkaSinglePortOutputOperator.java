package com.datatorrent.malhar.contrib.kafka;

import com.datatorrent.api.DefaultInputPort;
import kafka.producer.KeyedMessage;

public class KafkaSinglePortOutputOperator<K, V> extends AbstractKafkaOutputOperator<K, V>
{

  /**
   * This input port receives tuples that will be written out to Kafka.
   */
  public final transient DefaultInputPort<V> inputPort = new DefaultInputPort<V>()
  {
    @Override
    public void process(V tuple)
    {
      // Send out single data
      getProducer().send(new KeyedMessage<K, V>(getTopic(), tuple));
      sendCount++;

      // TBD: Kafka also has an api to send out bunch of data in a list.
      // which is not yet supported here.

      //logger.debug("process message {}", tuple.toString());
    }
  };

}
