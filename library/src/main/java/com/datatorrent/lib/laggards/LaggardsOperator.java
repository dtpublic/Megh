/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.lib.laggards;

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
  private transient GetterLong getter = null;

  protected Class<?> clazz = null;

  protected transient long bufferTime = 60;             // Default: 1 min buffer time
  protected transient long tumblingWindowTime = 3600;   // Default: 1 Hour Window/Bucket time
  protected transient long laggardsWindowTime = 900;    // Default: 15 mins laggards time
  protected transient String timestampKeyName = "time";

  protected transient long referenceTime = 0;
  protected transient long laggardsStartTime = 0;
  protected transient long currentWindowStartTime = 0;

  protected transient long totalTuples = 0;
  protected transient long normalTuples = 0;
  protected transient long laggardsTuples = 0;
  protected transient long lateLaggardsTuples = 0;
  protected transient long errorTuples = 0;

  @AutoMetric
  private float percentNormal = 0;

  @AutoMetric
  private float percentLaggards = 0;

  @AutoMetric
  private float percentLateLaggards = 0;

  @AutoMetric
  private float percentErrors = 0;

  /**
   * checkSetClazz: Check if all input/ouput ports have same schema class or not.
   */
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

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> normal = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> laggards = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };


  @OutputPortFieldAnnotation(schemaRequired = true)
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

    logger.debug("bufferTime {} tumblingWindowTime {} laggardsWindowTime {}, timestampKeyName {}",
        bufferTime, tumblingWindowTime, laggardsWindowTime, timestampKeyName);
  }

  @Override
  public void endWindow()
  {
    if (totalTuples != 0) {
      percentNormal = (float)(normalTuples * 100) / totalTuples;
      percentLaggards = (float)(laggardsTuples * 100) / totalTuples;
      percentLateLaggards = (float)(lateLaggardsTuples * 100) / totalTuples;
      percentErrors = (float)(errorTuples * 100) / totalTuples;

      logger.debug("referenceTime {} currentWindowStartTime {} laggardsStartTime {}",
          referenceTime, currentWindowStartTime, laggardsStartTime);
      logger.debug("totalTuples {} errorTuples {} lateLaggardsTuples {} normalTuples {} laggardsTuples {}",
          totalTuples, errorTuples, lateLaggardsTuples, normalTuples, laggardsTuples);
      logger.debug("percentNormal {} percentLaggards {} percentLateLaggards {} percentErrors {}",
          percentNormal, percentLaggards, percentLateLaggards, percentErrors);
    }
  }

  /**
   * updateTime: updates the current referenceTime and adjust the tumbling and laggards window accordingly.
   */
  protected void updateTime()
  {
    long currentTime = System.currentTimeMillis() / 1000L;
    if (currentTime == referenceTime) {
      return;
    }

    referenceTime = currentTime;
    currentWindowStartTime = referenceTime - (referenceTime % tumblingWindowTime);
    laggardsStartTime = currentWindowStartTime - (tumblingWindowTime > laggardsWindowTime ? tumblingWindowTime : laggardsWindowTime);
  }

  /**
   * checkLaggards: Check and emit if incoming tuple is in Normal Window, Laggards Window or Error
   */
  private void checkLaggards(Object t)
  {
    totalTuples++;
    if (getter == null) {
      getter = (GetterLong)PojoUtils.constructGetter(clazz, timestampKeyName, long.class);
    }
    long tm = getter.get(t);

    updateTime();

    if (tm > (referenceTime + bufferTime)) {
      error.emit(t);
      errorTuples++;
    } else if (tm < laggardsStartTime) {
      error.emit(t);
      lateLaggardsTuples++;
      errorTuples++;
    } else if (tm >= currentWindowStartTime) {
      normal.emit(t);
      normalTuples++;
    } else {
      laggards.emit(t);
      laggardsTuples++;
    }
  }

  /**
   * getBufferTime: Get Early Arrival Window time
   */
  public long getBufferTime()
  {
    return bufferTime;
  }

  /**
   * setBufferTime: Set Early Arrival Window time
   */
  public void setBufferTime(long _bufferTime)
  {
    bufferTime = _bufferTime;
  }

  /**
   * getTumblingWindowTime: Get Tumbling Window Time
   */
  public long getTumblingWindowTime()
  {
    return tumblingWindowTime;
  }

  /**
   * setTumblingWindowTime: Set Tumbling Window Time
   */
  public void setTumblingWindowTime(long _tumblingWindowTime)
  {
    tumblingWindowTime = _tumblingWindowTime;
  }

  /**
   * getLaggardsWindowTime: Get Laggards Window Time
   */
  public long getLaggardsWindowTime()
  {
    return laggardsWindowTime;
  }

  /**
   * setLaggardsWindowTime: Set Laggards Window Time
   */
  public void setLaggardsWindowTime(long _laggardsWindowTime)
  {
    laggardsWindowTime = _laggardsWindowTime;
  }

  /**
   * getTimestampKeyName: Get Timestamp Field Key Name
   */
  public String getTimestampKeyName()
  {
    return timestampKeyName;
  }

  /**
   * setTimestampKeyName: Set Timestamp Field Key Name
   */
  public void setTimestampKeyName(String _timestampKeyName)
  {
    timestampKeyName = _timestampKeyName;
  }

  private static final Logger logger = LoggerFactory.getLogger(LaggardsOperator.class);
}
