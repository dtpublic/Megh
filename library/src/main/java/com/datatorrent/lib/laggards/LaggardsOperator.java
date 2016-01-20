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

/**
 * Implementation of {@link #LaggardsOperator()}. LaggardsOperator takes tuples as an input
 * and help determine if that tuple meets the SLA based on tuple time and arrival time
 *
 * Input Ports {@link #input} - input tuples of type POJO object
 *
 * Output Ports {@link #normal} - tuples found valid in current window
 * emitted as POJO object {@link laggards} - tuples found valid in laggards
 * window emitted as POJO {@link #error} - that arrived later than laggards
 * or ahead in time emitted as POJO object
 *
 */
public class LaggardsOperator extends BaseOperator
{
  private transient GetterLong getter = null;

  protected Class<?> clazz = null;

  protected long bufferTime = 60;             // Default: 1 min buffer time
  protected long tumblingWindowTime = 3600;   // Default: 1 Hour Window/Bucket time
  protected long laggardsWindowTime = 900;    // Default: 15 mins laggards time
  protected String timestampKeyName = "time";

  protected transient long referenceTime = 0;
  protected transient long laggardsStartTime = 0;
  protected transient long currentWindowStartTime = 0;

  @AutoMetric
  protected long totalTuples = 0;

  @AutoMetric
  protected long normalTuples = 0;

  @AutoMetric
  protected long laggardsTuples = 0;

  @AutoMetric
  protected long lateLaggardsTuples = 0;

  @AutoMetric
  protected long errorTuples = 0;

  /**
   * checkSetClazz: Check if all input/ouput ports have same schema class or not.
   */
  private void checkSetClazz(PortContext context)
  {
    if (clazz == null) {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    } else if (clazz != context.getValue(Context.PortContext.TUPLE_CLASS)) {
      logger.error("Input/Output ports must have same schema class. {} != {}",
          clazz, context.getValue(Context.PortContext.TUPLE_CLASS));
    }
  }

  /**
   * {@link #input} input tuples of type clazz
   */
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

  /**
   * {@link #normal} input tuples that were in current window
   */
  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> normal = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  /**
   * {@link #laggards} input tuples that were in laggards window
   */
  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> laggards = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  /**
   * {@link #error} input tuples that arrived later than laggards or ahead in time
   */
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
    logger.debug("bufferTime {} tumblingWindowTime {} laggardsWindowTime {}, timestampKeyName {}",
        bufferTime, tumblingWindowTime, laggardsWindowTime, timestampKeyName);
  }

  @Override
  public void beginWindow(long windowId)
  {
    totalTuples = normalTuples = laggardsTuples = lateLaggardsTuples = errorTuples = 0;
  }

  @Override
  public void endWindow()
  {
    if (totalTuples != 0) {
      logger.debug("totalTuples {} errorTuples {} lateLaggardsTuples {} normalTuples {} laggardsTuples {}",
          totalTuples, errorTuples, lateLaggardsTuples, normalTuples, laggardsTuples);
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

    long currentWindowStart = referenceTime - (referenceTime % tumblingWindowTime);
    if(currentWindowStart == currentWindowStartTime) {
      return;
    }

    currentWindowStartTime = currentWindowStart;
    laggardsStartTime = currentWindowStartTime - (tumblingWindowTime > laggardsWindowTime ? tumblingWindowTime : laggardsWindowTime);
    logger.debug("referenceTime {} currentWindowStartTime {} laggardsStartTime {}",
        referenceTime, currentWindowStartTime, laggardsStartTime);
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
