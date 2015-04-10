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

  void produceMsg(String brokerURL, int numMessages) throws Exception
  {
    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);

    // Create a Connection
    Connection connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

    // Create the destination (Topic or Queue)
    Destination destination = session.createQueue("TEST.FOO");

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
      new JMSMessageProducer().produceMsg("tcp://localhost:61616", 1000);
  }
}
