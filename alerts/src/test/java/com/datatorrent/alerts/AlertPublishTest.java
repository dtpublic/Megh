package com.datatorrent.alerts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.netlet.util.Slice;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class AlertPublishTest
{
  private final class RabbitMQAlertMessageGenerator implements AlertPublisherInterface
  {
    private final KryoSerializableStreamCodec<Message> codec = new KryoSerializableStreamCodec<Message>();

    ConnectionFactory connFactory = new ConnectionFactory();
    Connection connection = null;
    Channel channel = null;
    final String exchange = "testEx";

    public boolean configureAlertPublishing()
    {
      try {
        connFactory.setHost("localhost");
        connection = connFactory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchange, "fanout");
      } catch (Exception e) {
        DTThrowable.wrapIfChecked(e);
      }

      return true;
    }

    @Override
    public boolean publishAlert(Message alert)
    {
      System.out.println("Publishing alert");
      Slice msg = codec.toByteArray(alert);
      try {
        channel.basicPublish(exchange, "", null, msg.toByteArray());
      } catch (IOException e) {
        DTThrowable.wrapIfChecked(e);
      }

      return false;
    }

    public void teardown()
    {
      try {
        channel.close();
        connection.close();
      } catch (IOException e) {
        DTThrowable.wrapIfChecked(e);
      }
    }

    public void publishAlertMessages(int testNum)
    {
      for (Integer i = 0; i < testNum; i++) {
        Message message = new Message();

        message.setFlag(true);
        message.setEventId(i.toString());
        message.setCurrentLevel(1);
        message.setAppId("Dummy_application");

        publishAlert(message);
      }
    }
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);

    public static class CollectorInputPort<T> extends DefaultInputPort<T>
    {
      public volatile static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
      ArrayList<T> list;

      final String id;

      public CollectorInputPort(String id, Operator module)
      {
        super();
        this.id = id;
      }

      @Override
      public void process(T tuple)
      {
        list.add(tuple);
      }

      @Override
      public void setConnected(boolean flag)
      {
        if (flag) {
          collections.put(id, list = new ArrayList<T>());
        }
      }
    }
  }

  @Test
  public void runTest() throws IOException
  {
    final int testNum = 10;
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    AlertsReceiver consumer = dag.addOperator("Consumer", new AlertsReceiver());
    consumer.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    final CollectorModule<Message> collector = dag.addOperator("Collector", new CollectorModule<Message>());

    consumer.setHost("localhost");
    consumer.setExchange("testEx");
    consumer.setExchangeType("fanout");

    final RabbitMQAlertMessageGenerator publisher = new RabbitMQAlertMessageGenerator();
    publisher.configureAlertPublishing();

    dag.addStream("Stream", consumer.messageOutput, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 10000L;
        try {
          while (!collector.inputPort.collections.containsKey("collector") && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
          publisher.publishAlertMessages(testNum);
          startTms = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTms < timeout) {
            List<?> list = collector.inputPort.collections.get("collector");

            if (list.size() < testNum) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        } finally {
          lc.shutdown();
        }
      }

    }.start();

    lc.run();

    System.out.println("collection size: " + collector.inputPort.collections.size() + collector.inputPort.collections);
  }
}
