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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator to read data from a topic in a solace appliance.
 * Multiple partitions can be used to read data parallely from the same topic.
 */
public abstract class AbstractSolaceDirectInputOperator<T> extends AbstractSolaceBaseInputOperator<T> implements InputOperator, Operator.ActivationListener<com.datatorrent.api.Context.OperatorContext>
{
  @NotNull
  protected String topicName;

  private transient Topic topic;

  private transient List<T> tuples = new ArrayList<T>();

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    super.setup(context);
    // Create a handle to topic locally
    topic = factory.createTopic(topicName);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId <= lastCompletedWId) {
      try {
        tuples = (List<T>)idempotentStorageManager.load(operatorId, windowId);
        handleRecovery();
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    try {
      if (windowId > lastCompletedWId) {
        idempotentStorageManager.save(tuples, operatorId, windowId);
      }
      tuples.clear();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void emitTuples()
  {
    if (windowId > lastCompletedWId) {
      super.emitTuples();
    }
  }

  @Override
  protected Consumer getConsumer() throws JCSMPException
  {
    session.addSubscription(topic);
    return session.getMessageConsumer((XMLMessageListener)null);
  }

  @Override
  protected void clearConsumer() throws JCSMPException
  {
    session.removeSubscription(topic);
  }

  @Override
  protected T processMessage(BytesXMLMessage message)
  {
    T tuple = super.processMessage(message);
    tuples.add(tuple);
    return tuple;
  }

  private void handleRecovery() {
    for (T tuple : tuples) {
      //processDirectMessage(message);
      emitTuple(tuple);
    }
    tuples.clear();
  }

  public String getTopicName()
  {
    return topicName;
  }

  public void setTopicName(String topicName)
  {
    this.topicName = topicName;
  }
}
