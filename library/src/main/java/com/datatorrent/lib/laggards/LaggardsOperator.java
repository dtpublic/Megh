/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.lib.laggards;

import java.util.Date;

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
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * Implementation of LaggardsOperator takes tuples as an input
 * and help determine if that tuple meets the SLA based on tuple time and arrival time
 *
 * Input Ports {@link #in} - input tuples of type POJO object
 *
 * Output Ports {@link #normal} - tuples found valid in current window
 * emitted as POJO object {@link #laggards} - tuples found valid in laggards
 * window emitted as POJO {@link #error} - that arrived later than laggards
 * or ahead in time emitted as POJO object
 *
 */
public class LaggardsOperator extends BaseOperator
{
  private transient Getter getter = null;

  private Class<?> clazz = null;

  private long bufferTime = 60 * 1000;             // Default: 1 min buffer time
  private long tumblingWindowTime = 3600 * 1000;   // Default: 1 Hour Window/Bucket time
  private long laggardsWindowTime = 900 * 1000;    // Default: 15 minutes laggards time
  private String timestampKeyName = "time";

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
      LOG.error("Input/Output ports must have same schema class. {} != {}",
          clazz, context.getValue(Context.PortContext.TUPLE_CLASS));
    }
  }

  /**
   * in input tuples of type clazz
   */
  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
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
   * normal input tuples that were in current window
   */
  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> normal = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  /**
   * laggards input tuples that were in laggards window
   */
  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> laggards = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  /**
   * error input tuples that arrived later than laggards or ahead in time
   */
  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(PortContext context)
    {
      checkSetClazz(context);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    LOG.debug("bufferTime {} tumblingWindowTime {} laggardsWindowTime {}, timestampKeyName {}",
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
      LOG.debug("totalTuples {} errorTuples {} lateLaggardsTuples {} normalTuples {} laggardsTuples {}",
          totalTuples, errorTuples, lateLaggardsTuples, normalTuples, laggardsTuples);
    }
  }

  /**
   * fetchCurrentTime: Fetch current time, this could be a System Time as well, depending on the setup.
   * In case of batch job support, override this method and provide appropriate relative time
   *
   * @return Current reference time i.e. Current System Time
   */
  protected long fetchCurrentTime()
  {
    return System.currentTimeMillis();
  }

  /**
   * updateTime: updates the current referenceTime and adjust the tumbling and laggards window accordingly
   * referenceTime is used to determine the Current/Normal Window
   * For streaming applications, referenceTime is nothing but the Current System Time
   */
  private void updateTime()
  {
    long currentTime = fetchCurrentTime();
    if (currentTime == referenceTime) {
      return;
    }

    referenceTime = currentTime;

    long currentWindowStart = referenceTime - (referenceTime % tumblingWindowTime);
    if (currentWindowStart == currentWindowStartTime) {
      return;
    }

    currentWindowStartTime = currentWindowStart;
    laggardsStartTime = currentWindowStartTime - (tumblingWindowTime > laggardsWindowTime ? tumblingWindowTime : laggardsWindowTime);
    LOG.debug("referenceTime {} currentWindowStartTime {} laggardsStartTime {}",
        referenceTime, currentWindowStartTime, laggardsStartTime);
  }

  /**
   * checkLaggards: Check and emit if incoming tuple is in Normal Window, Laggards Window or Error
   *
   *                       t1                  t2                              t3         t4
   * ------ Late/Error -----|- Laggards Window -|---- Current/Normal Window ----|- Buffer -|---
   *                                                                        ^
   *                                                                   referenceTime
   * 
   * Current/Normal Window is determined by the referenceTime like for streaming apps its current system time
   *
   * If time in tuple
   * - is less than t1 then it is late laggards and emitted on error port
   * - is between t1, t2 then it is laggards and emitted on laggards port
   * - is between t2, t3 then it is normal and emitted on normal port
   * - is between t3, t4 then it is ahead in time, however considered normal and emitted on normal port
   * - is greater than t4 then it is too much ahead in time and shall be emitted on error port
   */
  private void checkLaggards(Object t)
  {
    totalTuples++;
    if (getter == null) {
      getter = PojoUtils.createGetter(clazz, timestampKeyName, Date.class);
    }

    long tm = ((Date)getter.get(t)).getTime();

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
   *
   * @return bufferTime parameter value, early arrival window time (in milliseconds)
   */
  public long getBufferTime()
  {
    return bufferTime;
  }

  /**
   * setBufferTime: Set Early Arrival Window time
   *
   * @param bufferTime parameter, early arrival window time (in milliseconds)
   */
  public void setBufferTime(long bufferTime)
  {
    this.bufferTime = bufferTime;
  }

  /**
   * getTumblingWindowTime: Get Tumbling Window Time
   *
   * @return tumblingWindowTime Tumbling Window Time (in milliseconds)
   */
  public long getTumblingWindowTime()
  {
    return tumblingWindowTime;
  }

  /**
   * setTumblingWindowTime: Set Tumbling Window Time
   *
   * @param tumblingWindowTime Tumbling Window Time (in milliseconds)
   */
  public void setTumblingWindowTime(long tumblingWindowTime)
  {
    this.tumblingWindowTime = tumblingWindowTime;
  }

  /**
   * getLaggardsWindowTime: Get Laggards Window Time
   *
   * @return laggardsWindowTime Laggards Window Time (in milliseconds)
   */
  public long getLaggardsWindowTime()
  {
    return laggardsWindowTime;
  }

  /**
   * setLaggardsWindowTime: Set Laggards Window Time
   *
   * @param laggardsWindowTime Laggards Window Time (in milliseconds)
   */
  public void setLaggardsWindowTime(long laggardsWindowTime)
  {
    this.laggardsWindowTime = laggardsWindowTime;
  }

  /**
   * getTimestampKeyName: Get Timestamp Field Key Name
   *
   * @return timestampKeyName parameter, timestamp field in incoming tuple/POJO
   */
  public String getTimestampKeyName()
  {
    return timestampKeyName;
  }

  /**
   * setTimestampKeyName: Set Timestamp Field Key Name
   * 
   * @param timestampKeyName timestamp field name in incoming tuple/POJO
   */
  public void setTimestampKeyName(String timestampKeyName)
  {
    this.timestampKeyName = timestampKeyName;
  }

  private static final Logger LOG = LoggerFactory.getLogger(LaggardsOperator.class);
}
