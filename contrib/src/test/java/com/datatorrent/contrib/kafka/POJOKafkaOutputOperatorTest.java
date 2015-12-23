package com.datatorrent.contrib.kafka;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;

public class POJOKafkaOutputOperatorTest extends KafkaOperatorTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(POJOKafkaOutputOperatorTest.class);
  private static final int maxTuple = 20;
  private static CountDownLatch latch;

  /**
   * Test AbstractKafkaOutputOperator (i.e. an output adapter for Kafka, aka producer).
   * This module sends data into kafka message bus.
   *
   * [Generate tuple] ==> [send tuple through Kafka output adapter(i.e. producer) into Kafka message bus]
   * ==> [receive data in outside Kaka listener (i.e consumer)]
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testKafkaOutputOperator() throws Exception
  {
    //initialize the latch to synchronize the threads
    latch = new CountDownLatch(maxTuple);
    // Setup a message listener to receive the message
    KafkaTestConsumer listener = new KafkaTestConsumer("topic1");
    listener.setLatch(latch);
    new Thread(listener).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    KafkaOutputOperatorTest.StringGeneratorInputOperator generator = dag.addOperator("TestStringGenerator", KafkaOutputOperatorTest.StringGeneratorInputOperator.class);
    POJOKafkaOutputOperator node = dag.addOperator("KafkaMessageProducer", POJOKafkaOutputOperator.class);

    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("producer.type", "async");
    props.setProperty("queue.buffering.max.ms", "200");
    props.setProperty("queue.buffering.max.messages", "10");

    node.setConfigProperties(props);
    node.setTopic("topic1");
    node.setBrokerList("localhost:9092");
    node.setBatchSize(5);

    // Connect ports
    dag.addStream("Kafka message", generator.outputPort, node.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);

    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    // Immediately return unless latch timeout in 15 seconds
    latch.await(15, TimeUnit.SECONDS);
    lc.shutdown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, listener.holdingBuffer.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.holdingBuffer.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.getMessage(listener.holdingBuffer.peek()));

    listener.close();
  }
}
