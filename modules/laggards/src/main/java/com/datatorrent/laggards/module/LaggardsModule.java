/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.modules.laggards;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

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

  /**
   * {@link #input} input tuples of type byte[]
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

    dag.getMeta(operator).getMeta(operator.in).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
    dag.getMeta(operator).getMeta(operator.normal).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
    dag.getMeta(operator).getMeta(operator.laggards).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
    dag.getMeta(operator).getMeta(operator.error).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);

    input.set(operator.in);
    normal.set(operator.normal);
    laggards.set(operator.laggards);
    error.set(operator.error);
  }

  public String getPojoSchema()
  {
    return pojoSchema;
  }

  public void setPojoSchema(String pojoSchema)
  {
    this.pojoSchema = pojoSchema;
  }
}
