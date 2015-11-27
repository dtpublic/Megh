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

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.netlet.util.DTThrowable;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

/**
 * Operator to read data from an endpoint in a solace appliance.
 * Multiple partitions can be used to read data parallely from the same endpoint.
 */
public abstract class AbstractSolaceGuaranteedInputOperator<T> extends AbstractSolaceBaseInputOperator<T> implements InputOperator, Operator.ActivationListener<Context.OperatorContext>
{

  @NotNull
  protected String endpointName;
  @NotNull
  protected EndpointType endpointType = EndpointType.QUEUE;

  @NotNull
  protected EndpointProperties endpointProperties = new EndpointProperties();

  protected long endpointProvisionFlags = JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS;

  private transient Endpoint endpoint;

  private transient long windowTime;

  private transient WindowMessageInfo mesgInfo = new WindowMessageInfo();
  // BytesXmlMessage is not serializable so not persisting recent message
  // If application window doesn't coincide with checkpoint window we wont have recent message and need a different way
  // to get message id, hence persisting recent message id
  private transient BytesXMLMessage recentMessage = null;
  private long recentMesgId;
  // Pending message from a previous window that was used determine the limit of message in a window during recovery
  private transient BytesXMLMessage pendingMessage = null;
  private transient TreeMap<Long, BytesXMLMessage> lastMessages = new TreeMap<Long, BytesXMLMessage>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowTime = context.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    // Create a handle to endpoint locally
    if (endpointType == EndpointType.QUEUE) {
      endpoint = factory.createQueue(this.endpointName);
    } else {
      endpoint = factory.createDurableTopicEndpointEx(endpointName);
    }
  }

  protected Consumer getConsumer() throws JCSMPException
  {
    session.provision(endpoint, endpointProperties, endpointProvisionFlags);
    ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
    consumerFlowProperties.setEndpoint(endpoint);
    consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
    return session.createFlow(null, consumerFlowProperties, endpointProperties);
  }

  @Override
  protected void clearConsumer() throws JCSMPException
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId > lastCompletedWId) {
      mesgInfo.firstMesgId = -1;
      mesgInfo.lastMesgId = -1;
    } else {
      try {
        mesgInfo = (WindowMessageInfo)idempotentStorageManager.load(operatorId, windowId);
        handleRecovery();
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void emitTuples()
  {
    if (windowId > lastCompletedWId) {
      if (pendingMessage == null) {
        super.emitTuples();
      } else {
        processMessage(pendingMessage);
        pendingMessage = null;
      }
    }
  }

  @Override
  public T processMessage(BytesXMLMessage message)
  {
    T tuple = super.processMessage(message);
    recentMessage = message;
    recentMesgId = recentMessage.getMessageIdLong();
    if (mesgInfo.firstMesgId == -1) {
      mesgInfo.firstMesgId = recentMesgId;
    }
    //message.ackMessage();
    return tuple;
  }

  protected void handleRecovery() {
    boolean done = false;
    while (!done) {
      try {
        BytesXMLMessage message = consumer.receive((int)windowTime);
        if (message != null) {
          if ((message.getMessageIdLong() >= mesgInfo.firstMesgId) && (message.getMessageIdLong() <= mesgInfo.lastMesgId)) {
            processMessage(message);
          } else if (message.getMessageIdLong() > mesgInfo.lastMesgId) {
            pendingMessage = message;
            done = true;
          }
        } else {
          done = true;
        }
      } catch (JCSMPException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    mesgInfo.lastMesgId = recentMesgId;
    // Store last message to acknowledge on committed
    if (recentMessage != null) {
      lastMessages.put(windowId, recentMessage);
      //recentMessage.ackMessage();
    }
    try {
      idempotentStorageManager.save(mesgInfo, operatorId, windowId);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void committed(long window)
  {
    BytesXMLMessage message = lastMessages.get(windowId);
    // Acknowledge the message
    if (message != null) {
      message.ackMessage();
    }
    Set<Long> windows = lastMessages.keySet();
    Iterator<Long> iterator = windows.iterator();
    while (iterator.hasNext()) {
      if (iterator.next() <= window) {
        iterator.remove();
      } else {
        break;
      }
    }
    super.committed(window);
  }

  private static class WindowMessageInfo {
    long firstMesgId = -1;
    long lastMesgId = -1;
  }

  public String getEndpointName()
  {
    return endpointName;
  }

  public void setEndpointName(String endpointName)
  {
    this.endpointName = endpointName;
  }

  public EndpointType getEndpointType()
  {
    return endpointType;
  }

  public void setEndpointType(EndpointType endpointType)
  {
    this.endpointType = endpointType;
  }

  public EndpointProperties getEndpointProperties()
  {
    return endpointProperties;
  }

  public void setEndpointProperties(EndpointProperties endpointProperties)
  {
    this.endpointProperties = endpointProperties;
  }

  public long getEndpointProvisionFlags()
  {
    return endpointProvisionFlags;
  }

  public void setEndpointProvisionFlags(long endpointProvisionFlags)
  {
    this.endpointProvisionFlags = endpointProvisionFlags;
  }

}
