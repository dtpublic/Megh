/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.solace;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.*;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

/**
 *
 */
public class SolaceOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(SolaceOperatorTest.class);

  @Test
  public void testGuaranteedOperator() {
    JCSMPProperties properties = new JCSMPProperties();
    //properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.HOST, "192.168.1.168:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");
    properties.setProperty(JCSMPProperties.ACK_EVENT_MODE, JCSMPProperties.SUPPORTED_ACK_EVENT_MODE_WINDOWED);
    SolacePublisher publisher = new SolacePublisher(properties, "MyQ", new EndpointProperties());
    SolaceGuaranteedTextStrInputOperator inputOperator = new SolaceGuaranteedTextStrInputOperator();
    try {
      publisher.connect();
      publisher.publish();

      inputOperator.setProperties(properties);
      inputOperator.setEndpointName("MyQ");

      CollectorTestSink sink = new CollectorTestSink();
      inputOperator.output.setSink(sink);

      Attribute.AttributeMap map = new Attribute.AttributeMap.DefaultAttributeMap();
      map.put(Context.OperatorContext.SPIN_MILLIS, 10);
      Context.OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(0, map);
      inputOperator.setup(context);
      inputOperator.activate(context);

      inputOperator.beginWindow(0);
      while (!publisher.published) {
        inputOperator.emitTuples();
      }
      inputOperator.endWindow();

      inputOperator.deactivate();
      inputOperator.teardown();

      logger.info("TOTAL TUPLES {}", sink.collectedTuples.size());
      /*
      for (String s : ((CollectorTestSink<String>)sink).collectedTuples) {
        logger.debug("Received {}", s);
      }
      */
      Assert.assertEquals("Received tupes", publisher.publishCount, sink.collectedTuples.size());
    } catch (JCSMPException e) {
      Assert.fail(e.getMessage());
    } finally {
      publisher.disconnect();
    }
  }

  @Test
  public void testDirectOperator() {
    JCSMPProperties properties = new JCSMPProperties();
    //properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.HOST, "192.168.1.168:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");
    properties.setProperty(JCSMPProperties.ACK_EVENT_MODE, JCSMPProperties.SUPPORTED_ACK_EVENT_MODE_WINDOWED);
    SolacePublisher publisher = new SolacePublisher(properties, "topic1", null);
    publisher.publishCount = 10000;
    SolaceDirectTextStrInputOperator inputOperator = new SolaceDirectTextStrInputOperator();
    try {
      publisher.connect();
      publisher.publish();

      inputOperator.setProperties(properties);
      inputOperator.setTopicName("topic1");

      CollectorTestSink sink = new CollectorTestSink();
      inputOperator.output.setSink(sink);

      Attribute.AttributeMap map = new Attribute.AttributeMap.DefaultAttributeMap();
      map.put(Context.OperatorContext.SPIN_MILLIS, 10);
      map.put(Context.DAGContext.APPLICATION_PATH, "target/" + this.getClass().getName());
      Context.OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(0, map);
      inputOperator.setup(context);
      inputOperator.activate(context);

      inputOperator.beginWindow(0);
      while (!publisher.published) {
        inputOperator.emitTuples();
      }
      for (int i = 0; i < 1000; ++i) {
        inputOperator.emitTuples();
      }
      inputOperator.endWindow();

      inputOperator.deactivate();
      inputOperator.teardown();

      logger.info("TOTAL TUPLES {}", sink.collectedTuples.size());
      /*
      for (String s : ((CollectorTestSink<String>)sink).collectedTuples) {
        logger.debug("Received {}", s);
      }
      */
      Assert.assertEquals("Received tupes", publisher.publishCount, sink.collectedTuples.size());
    } catch (JCSMPException e) {
      Assert.fail(e.getMessage());
    } finally {
      publisher.disconnect();
    }
  }

  /*
  @Test
  public void testDirectSolace() {
    JCSMPProperties properties = new JCSMPProperties();
    properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");

    final JCSMPFactory factory = JCSMPFactory.onlyInstance();
    JCSMPSession session = null;
    try {
      session = factory.createSession(properties);
      session.connect();
      logger.info("Compression capable {}", session.getCapability(CapabilityType.COMPRESSION));
      Topic topic = factory.createTopic("topic1");
      TextMessage message = factory.createMessage(TextMessage.class);
      message.setText("Hello World");
      session.addSubscription(topic);
      XMLMessageConsumer messageConsumer = session.getMessageConsumer(new XMLMessageListener()
      {
        @Override
        public void onReceive(BytesXMLMessage bytesXMLMessage)
        {
          //logger.debug("Recieved message {}", ((TextMessage)bytesXMLMessage).getText());
          logger.debug("Length {}", bytesXMLMessage.getBytes().length);
          byte[] bytes = new byte[bytesXMLMessage.getAttachmentContentLength()];
          bytesXMLMessage.readAttachmentBytes(bytes);
          logger.debug("Text {}", new String(bytes));
        }

        @Override
        public void onException(JCSMPException e)
        {
          logger.error("Recieve exception ", e);
        }
      });
      XMLMessageProducer messageProducer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
      {
        @Override
        public void handleError(String s, JCSMPException e, long l)
        {
          logger.error("Exception ", e);
        }

        @Override
        public void responseReceived(String s)
        {
          logger.debug("Response recieved {}", s);
        }
      });
      messageConsumer.start();
      messageProducer.send(message, topic);
      Thread.sleep(30000);
      messageConsumer.stop();
    } catch (InvalidPropertiesException e) {
      e.printStackTrace();
    } catch (JCSMPException e) {
      e.printStackTrace();
      assert false;
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (session != null) {
        session.closeSession();
      }
    }
  }

  @Test
  public void testQueueSolace() {
    JCSMPProperties properties = new JCSMPProperties();
    properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");

    final JCSMPFactory factory = JCSMPFactory.onlyInstance();
    JCSMPSession session = null;
    try {
      session = factory.createSession(properties);
      session.connect();
      logger.info("Compression capable {}", session.getCapability(CapabilityType.COMPRESSION));
      EndpointProperties endpointProperties = new EndpointProperties();
      //endpointProperties.setPermission(EndpointProperties.PERMISSION_CONSUME);
      //endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
      Queue queue = factory.createQueue("MyQ");
      session.provision(queue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
      //Topic topic = factory.createTopic("topic1");
      TextMessage message = factory.createMessage(TextMessage.class);
      message.setText("Hello World");
      message.setDeliveryMode(DeliveryMode.PERSISTENT);
      /--*
      session.addSubscription(topic);
      XMLMessageConsumer messageConsumer = session.getMessageConsumer(new XMLMessageListener()
      {
        @Override
        public void onReceive(BytesXMLMessage bytes XMLMessage)
        {
          logger.debug("Recieved message {}", ((TextMessage)bytesXMLMessage).getText());
        }

        @Override
        public void onException(JCSMPException e)
        {
          logger.error("Recieve exception ", e);
        }
      });
      *--/
      XMLMessageProducer messageProducer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
      {
        @Override
        public void handleError(String s, JCSMPException e, long l)
        {
          logger.error("Exception ", e);
        }

        @Override
        public void responseReceived(String s)
        {
          logger.debug("Response recieved ", s);
        }
      });
      /--*
      messageConsumer.start();
      messageProducer.send(message, topic);
      Thread.sleep(30000);
      messageConsumer.stop();
      *--/
      messageProducer.send(message, queue);
      Thread.sleep(100);
      ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
      flowProperties.setEndpoint(queue);
      flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
      FlowReceiver receiver = session.createFlow(null, flowProperties, endpointProperties);
      receiver.start();
      BytesXMLMessage recvMessage = receiver.receive(30000);
      if (recvMessage != null) {
        logger.info("Received message : {}", recvMessage.dump());
        recvMessage.ackMessage();
      }
      receiver.close();
    } catch (InvalidPropertiesException e) {
      e.printStackTrace();
    } catch (JCSMPException e) {
      e.printStackTrace();
      assert false;
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (session != null) {
        session.closeSession();
      }
    }
  }
  */

}
