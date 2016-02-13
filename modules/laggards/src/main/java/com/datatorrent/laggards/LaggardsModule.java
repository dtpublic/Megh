/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.modules.laggards;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

import com.datatorrent.lib.laggards.LaggardsOperator;

/**
 * Implementation of {@link #LaggardsModule()}. Dag for this
 * module consists of just a single operator - LaggardsOperator
 *
 * Input Ports {@link #input} - input tuples of type POJO object
 *
 * Output Ports {@link #normal} - tuples found valid in current window
 * emitted as POJO object {@link laggards} - tuples found valid in laggards
 * window emitted as POJO {@link #error} - that arrived later than laggards
 * or ahead in time emitted as POJO object
 *
 */
public class LaggardsModule implements Module
{
  @NotNull
  private String pojoSchema;

  private long bufferTime = 0;
  private long tumblingWindowTime = 0;
  private long laggardsWindowTime = 0;
  private String timestampKeyName = null;

  /**
   * {@link #input} input tuples of type pojoSchema
   */
  public final transient ProxyInputPort<Object> input = new ProxyInputPort<Object>();

  /**
   * {@link #normal} input tuples that were in current window
   */
  public final transient ProxyOutputPort<Object> normal = new ProxyOutputPort<Object>();

  /**
   * {@link #laggards} input tuples that were in laggards window
   */
  public final transient ProxyOutputPort<Object> laggards = new ProxyOutputPort<Object>();

  /**
   * {@link #error} input tuples that arrived later than laggards or ahead in time
   */
  public final transient ProxyOutputPort<Object> error = new ProxyOutputPort<Object>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LaggardsOperator operator = dag.addOperator("LaggardsOperator", LaggardsOperator.class);
    Class<?> schemaClass;
    try {
      schemaClass = Class.forName(pojoSchema);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    operator.setBufferTime(bufferTime);
    operator.setTumblingWindowTime(tumblingWindowTime);
    operator.setLaggardsWindowTime(laggardsWindowTime);
    operator.setTimestampKeyName(timestampKeyName);

    dag.getMeta(operator).getMeta(operator.in).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
    dag.getMeta(operator).getMeta(operator.normal).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
    dag.getMeta(operator).getMeta(operator.laggards).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
    dag.getMeta(operator).getMeta(operator.error).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);

    input.set(operator.in);
    normal.set(operator.normal);
    laggards.set(operator.laggards);
    error.set(operator.error);
  }

  /**
   * getPojoSchema: Get Pojo Schema Class Name
   */
  public String getPojoSchema()
  {
    return pojoSchema;
  }

  /**
   * setPojoSchema: Set Pojo Schema Class Name
   */
  public void setPojoSchema(String pojoSchema)
  {
    this.pojoSchema = pojoSchema;
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

  private static final Logger logger = LoggerFactory.getLogger(LaggardsModule.class);
}
