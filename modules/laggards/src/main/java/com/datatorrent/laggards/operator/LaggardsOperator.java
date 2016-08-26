/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.modules.laggards;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterLong;

public class LaggardsOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(LaggardsOperator.class);
  protected Class<?> clazz;

  private transient long bufferTime = 0;
  private transient long windowTime = 0;
  private transient long laggardsTime = 0;
  private transient String timestampKeyname = null;

  private transient long referenceTime = 0;
  private transient long laggardsStartTime = 0;
  private transient long currentWindowStartTime = 0;

  private transient GetterLong getter = null;

  private transient long totalTuples = 0;
  private transient long normalTuples = 0;
  private transient long laggardsTuples = 0;
  private transient long lateLaggardsTuples = 0;
  private transient long errorTuples = 0;

  @AutoMetric
  private float percentNormal = 0;

  @AutoMetric
  private float percentLaggards = 0;

  @AutoMetric
  private float percentLateLaggards = 0;

  @AutoMetric
  private float percentErrors = 0;

  private void checkSetClazz(PortContext context)
  {
    if (clazz != null) {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    } else if (clazz != context.getValue(Context.PortContext.TUPLE_CLASS)) {
      logger.error("Input/Output ports must have same schema class. {} != {}",
          clazz, context.getValue(Context.PortContext.TUPLE_CLASS));
    }
  }

  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }

    @Override
    public void process(Object t)
    {
      checkLaggards(t);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true, optional = true)
  public final transient DefaultOutputPort<Object> normal = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true, optional = true)
  public final transient DefaultOutputPort<Object> laggards = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };


  @OutputPortFieldAnnotation(schemaRequired = true, optional = true)
  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    bufferTime = 60;        // Default: 1 min buffer time
    windowTime = 60 * 60;   // Default: 1 Hour Window/Bucket time
    laggardsTime = 15 * 60; // Default: 15 mins laggards time
    timestampKeyname = "time";
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    totalTuples = normalTuples = laggardsTuples = lateLaggardsTuples = errorTuples = 0;
    percentNormal = percentLaggards = percentLateLaggards = percentErrors = 0.0f;
  }

  @Override
  public void endWindow()
  {
    if (totalTuples != 0) {
      percentNormal = (float)(normalTuples * 100) / totalTuples;
      percentLaggards = (float)(laggardsTuples * 100) / totalTuples;
      percentLateLaggards = (float)(lateLaggardsTuples * 100) / totalTuples;
      percentErrors = (float)(errorTuples * 100) / totalTuples;

      logger.debug("totalTuples {} errorTuples {} lateLaggardsTuples {} normalTuples {} laggardsTuples {}",
          totalTuples, errorTuples, lateLaggardsTuples, normalTuples, laggardsTuples);
      logger.debug("percentNormal {} percentLaggards {} percentLateLaggards {} percentErrors {}",
          percentNormal, percentLaggards, percentLateLaggards, percentErrors);
    }
  }

  private void updateTime()
  {
    long currentTime = System.currentTimeMillis() / 1000L;
    if (currentTime == referenceTime) {
      return;
    }

    referenceTime = currentTime;
    currentWindowStartTime = referenceTime - (referenceTime % windowTime);
    laggardsStartTime = currentWindowStartTime - (windowTime > laggardsTime ? windowTime : laggardsTime);
  }

  private void checkLaggards(Object t)
  {
    totalTuples++;
    if (getter == null) {
      getter = (GetterLong)PojoUtils.constructGetter(clazz, timestampKeyname, long.class);
    }
    long tm = getter.get(t);

    updateTime();

    if (tm > (referenceTime + bufferTime)) {
      error.emit(t);
      errorTuples++;
    } else if (tm < laggardsStartTime) {
      error.emit(t);
      lateLaggardsTuples++;
    } else if (tm >= currentWindowStartTime) {
      normal.emit(t);
      normalTuples++;
    } else {
      laggards.emit(t);
      laggardsTuples++;
    }
  }
}
