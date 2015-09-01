package com.datatorrent.alerts;

import com.datatorrent.alerts.notification.email.EmailNotificationOperator;
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
    AlertsReceiver receiver = dag.addOperator("AlertsReceived", new AlertsReceiver());
    AlertsEngine responder = dag.addOperator("AlertsProcessed", new AlertsEngine());

    EmailNotificationOperator sendEmail = dag.addOperator("Email_Notify", new EmailNotificationOperator()) ;

    dag.addStream("ReceiverToEngine", receiver.messageOutput, responder.messageInput);
    dag.addStream("EngineToNotify", responder.messageOutput, sendEmail.input);
  }
}
