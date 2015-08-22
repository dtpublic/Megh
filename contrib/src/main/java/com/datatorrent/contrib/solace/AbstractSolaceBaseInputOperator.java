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

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.lib.io.IdempotentStorageManager;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointListener;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 *
 */
public abstract class AbstractSolaceBaseInputOperator<T> extends BaseOperator implements InputOperator, Operator.ActivationListener<Context.OperatorContext>, CheckpointListener
{

  @NotNull
  protected JCSMPProperties properties = new JCSMPProperties();

  protected IdempotentStorageManager idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();

  protected transient JCSMPFactory factory;
  protected transient JCSMPSession session;

  protected transient Consumer consumer;

  protected transient int operatorId;
  protected long windowId;
  protected transient long lastCompletedWId;

  int spinMillis;

  @Override
  public void setup(Context.OperatorContext context)
  {
    operatorId = context.getId();
    spinMillis = context.getValue(com.datatorrent.api.Context.OperatorContext.SPIN_MILLIS);
    factory = JCSMPFactory.onlyInstance();
    try {
      session = factory.createSession(properties);
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
    idempotentStorageManager.setup(context);
    lastCompletedWId = idempotentStorageManager.getLargestRecoveryWindow();
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    try {
      session.connect();
      consumer = getConsumer();
      consumer.start();
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void deactivate()
  {
    try {
      consumer.stop();
      clearConsumer();
      consumer.close();
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void teardown()
  {
    session.closeSession();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
  }

  @Override
  public void emitTuples()
  {
    try {
      BytesXMLMessage message = consumer.receive(spinMillis);
      if (message != null) {
        processMessage(message);
      }
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  protected T processMessage(BytesXMLMessage message)
  {
    T tuple = convert(message);
    emitTuple(tuple);
    return tuple;
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long window)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorId, window);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  protected abstract Consumer getConsumer() throws JCSMPException;

  protected abstract void clearConsumer() throws JCSMPException;

  protected abstract T convert(BytesXMLMessage message);

  protected abstract void emitTuple(T tuple);

  public JCSMPProperties getProperties()
  {
    return properties;
  }

  public void setProperties(JCSMPProperties properties)
  {
    this.properties = properties;
  }

  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return idempotentStorageManager;
  }

  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }

}
