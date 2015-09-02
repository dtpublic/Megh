package com.datatorrent.alerts;


import com.datatorrent.alerts.notification.email.EmailNotificationOperator;
import java.net.URI;

import com.datatorrent.api.DAG;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * @since 2.1.0
 */
@ApplicationAnnotation(name="AlertsApp")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    AlertPubSubReceiver receiver = dag.addOperator("AlertsReceived", new AlertPubSubReceiver());
    String gatewayAddress = "node0.morado.com:9292";
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    receiver.setUri(uri);
    receiver.setTopic("alerts");
    
    AlertsEngine responder = dag.addOperator("AlertsProcessed", new AlertsEngine());
    EmailNotificationOperator notify = dag.addOperator("Notify", new EmailNotificationOperator());

    dag.addStream("ReceiverToEngine", receiver.getMessageOutPort(), responder.messageInput);
//    dag.addStream("EngineToNotify", responder.messageOutput, notify.messageInput);

  }
}
