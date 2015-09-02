package com.datatorrent.alerts;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.WebsocketAppDataPusher;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;

public class PubSubReceiverTest
{
  public class AlertDataPushThread extends Thread
  {
    int testNum = 10;
    int eventId = 0;
    public final AlertMessageGenerator publisher;

    public AlertDataPushThread()
    {
      publisher = new AlertMessageGenerator();
      publisher.configureAlertPublishing();
    }

    @Override
    public void run()
    {
      while (testNum-- > 0) {
        try {
          publisher.publishAlertMessages(eventId++);
        } catch (Exception ex) {
          System.out.println("Error during pushing app data" + ex);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          System.out.println("Received interrupt, exiting app data push thread!");
          return;
        }
      }
    }

  }

  private final class AlertMessageGenerator implements PublisherInterface
  {
    private SharedPubSubWebSocketClient wsClient;
    private WebsocketAppDataPusher appDataPusher;

    public boolean configureAlertPublishing()
    {
      try {
        // Establish web socket connection
        String gatewayAddress = "node0.morado.com:9292";
        wsClient = new SharedPubSubWebSocketClient("ws://" + gatewayAddress + "/pubsub", 1500);
        wsClient.setLoginUrl("http://" + gatewayAddress + StreamingContainerManager.GATEWAY_LOGIN_URL_PATH);
        wsClient.setUserName("isha");
        wsClient.setPassword("isha");
        wsClient.setup();

        appDataPusher = new WebsocketAppDataPusher(wsClient, "alerts");

      } catch (Exception e) {
        DTThrowable.wrapIfChecked(e);
      }

      return true;
    }

    @Override
    public boolean publishAlert(Message alert)
    {
      System.out.println("Publishing alert");

      ObjectMapper mapper = new ObjectMapper();

      try {
        // Send message to alert gateway API
        String alertMessage = mapper.writeValueAsString(alert);
        JSONObject json = new JSONObject(alertMessage);
        appDataPusher.push(json);

      } catch (IOException e) {
        DTThrowable.wrapIfChecked(e);
      } catch (JSONException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      return false;
    }

    public void teardown()
    {
      try {
        // Close all the connections
      } catch (Exception e) {
        DTThrowable.wrapIfChecked(e);
      }
    }

    public void publishAlertMessages(int eventId)
    {
      Message message = new Message();

      message.setFlag(true);
      message.setEventId(Integer.toString(eventId));
      message.setCurrentLevel(1);
      message.setAppId("Dummy_application");

      publishAlert(message);
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
  public void runTest() throws IOException, InterruptedException
  {

    final AlertDataPushThread publisherThread = new AlertDataPushThread();

    final int testNum = 10;
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    PubSubReceiver consumer = dag.addOperator("Consumer", new PubSubReceiver());

    String gatewayAddress = "node0.morado.com:9292";
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    consumer.setUri(uri);
    consumer.setTopic("alerts");

    final CollectorModule<Message> collector = dag.addOperator("Collector", new CollectorModule<Message>());
    dag.addStream("Stream", consumer.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    try {
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
            publisherThread.start();
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

    } catch (Exception e) {
      DTThrowable.wrapIfChecked(e);
    }

    System.out.println("collection size: " + collector.inputPort.collections.size() + collector.inputPort.collections);
  }
}
