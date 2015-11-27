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

import com.datatorrent.netlet.util.DTThrowable;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SolacePublisher implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(SolacePublisher.class);
  final JCSMPFactory factory = JCSMPFactory.onlyInstance();
  JCSMPSession session = null;
  Destination destination;
  XMLMessageProducer messageProducer;
  ExecutorService executor;
  Random random = new Random();
  boolean published;
  int publishCount = 1000;
  int recvCount;

  JCSMPProperties properties;
  String destinationName;
  EndpointProperties endpointProperties;

  public SolacePublisher(JCSMPProperties properties, String destinationName, EndpointProperties endpointProperties) {
    this.properties = properties;
    this.destinationName = destinationName;
    this.endpointProperties = endpointProperties;
    executor = Executors.newSingleThreadExecutor();
  }

  public void connect() throws JCSMPException
  {
    try {
      session = factory.createSession(properties);
      session.connect();
      if (endpointProperties == null) {
        destination = factory.createTopic(destinationName);
      } else {
        destination = factory.createQueue(destinationName);
        session.provision((Endpoint) destination, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
      }
      messageProducer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
      {
        @Override
        public void handleError(String s, JCSMPException e, long l)
        {
          DTThrowable.rethrow(e);
        }

        @Override
        public void responseReceived(String s)
        {
          //logger.debug("Response recieved {}", s);
          if (++recvCount == publishCount) {
            published = true;
          }
        }
      });
    } catch (JCSMPException e) {
      disconnect();
      throw e;
    }
  }

  public void disconnect() {
    executor.shutdown();
    try {
      executor.awaitTermination(2000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      DTThrowable.rethrow(e);
    } finally {
      if (session != null) {
        session.closeSession();
      }
    }
  }

  public void publish() {
    recvCount = 0;
    published = false;
    executor.submit(this);
  }

  public void run() {
    for (int i = publishCount; i-- > 0;) {
      TextMessage message = factory.createMessage(TextMessage.class);
      message.setText("Hello World " + random.nextInt(1000));
      message.setDeliveryMode(DeliveryMode.DIRECT);
      try {
        messageProducer.send(message, destination);
      } catch (JCSMPException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

}
