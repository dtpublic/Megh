/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io.jms;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Class to produce JMS messages for testing JMS ingestion
 */
public class JMSMessageProducer
{
  String brokerURL;
  String subject;
  
  public JMSMessageProducer(String brokerURL, String subject)
  {
    this.brokerURL = brokerURL;
    this.subject = subject;
  }
  
  void produceMsg(int numMessages) throws Exception
  {
    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);

    // Create a Connection
    Connection connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // Create the destination (Topic or Queue)
    Destination destination = session.createQueue(subject);

    // Create a MessageProducer from the Session to the Topic or Queue
    MessageProducer producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    // Create a messages
    for (int i = 0; i < numMessages; i++) {
      String text = "Test Message : "+i;
      TextMessage message = session.createTextMessage(text);
      producer.send(message);
    }

    // Clean up
    session.close();
    connection.close();

  }

  public static void main(String[] args) throws Exception
  {
    JMSMessageProducer jmsMessageProducer = new JMSMessageProducer("tcp://localhost:61616", "TEST.FOO");
    jmsMessageProducer.produceMsg(1000);
  }
}
