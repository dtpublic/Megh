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

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

/**
 * Operator to send data to a topic in a solace appliance.
 */
public abstract class AbstractSolaceDirectOutputOperator<T> extends AbstractSolaceBaseOutputOperator<T>
{
  @NotNull
  protected String topicName;

  @Override
  protected Destination getDestination()
  {
    return factory.createTopic(topicName);
  }

  @Override
  protected Producer getProducer() throws JCSMPException
  {
    return session.getMessageProducer(null);
  }

  protected XMLMessage getMessage(T tuple) {
    XMLMessage message = convert(tuple);
    message.setDeliveryMode(DeliveryMode.DIRECT);
    return message;
  }

  protected abstract XMLMessage convert(T tuple);

  public String getTopicName()
  {
    return topicName;
  }

  public void setTopicName(String topicName)
  {
    this.topicName = topicName;
  }
}
