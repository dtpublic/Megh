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
    PubSubReceiver receiver = dag.addOperator("AlertsReceived", new PubSubReceiver());
    String gatewayAddress = "node0.morado.com:9292";
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    receiver.setUri(uri);
    receiver.setTopic("alerts");
    
    Engine responder = dag.addOperator("AlertsProcessed", new Engine());
    EmailNotificationOperator emailNotify = dag.addOperator("EmailNotify", new EmailNotificationOperator());

    dag.addStream("ReceiverToEngine", receiver.getMessageOutPort(), responder.messageInput);
    dag.addStream("EngineToNotify", responder.messageOutput, emailNotify.input);
  }
}
