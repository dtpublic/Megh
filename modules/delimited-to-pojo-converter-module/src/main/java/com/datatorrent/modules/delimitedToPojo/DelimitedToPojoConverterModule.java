/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedToPojo;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.delimited.DelimitedSchema;
import com.datatorrent.contrib.delimited.DelimitedToPojoConverterOperator;

/**
 * Implementation of {@link #DelimitedToPojoConverterModule()}. Dag for this
 * module consists of just a single operator - DelimitedToPojoConverterOperator
 * 
 * Input Ports {@link #input} - input tuples of type byte[]
 * 
 * Output Ports {@link #output} - input tuples found valid against the schema
 * emitted as byte[] {@link #error} - input tuples found valid against the
 * schema emitted as Map<String,Object>
 * 
 */
public class DelimitedToPojoConverterModule implements Module
{

  /**
   * Path of the schema- json file as specified in {@link DelimitedSchema}
   */
  @NotNull
  private String schemaPath;
  /**
   * complete class name of pojo
   */
  private String pojoSchema;
  /**
   * number of partions
   */
  private int numberOfPartitions;

  /**
   * {@link #input} input tuples of type byte[]
   */
  public final transient ProxyInputPort<byte[]> input = new ProxyInputPort<byte[]>();

  /**
   * {@link #output} input tuples that were successfully converted to pojo based
   * on pojoSchema
   */
  public final transient ProxyOutputPort<Object> output = new ProxyOutputPort<Object>();

  /**
   * {@link #error} input tuples that could not be converted to pojo based on
   * pojoSchema
   */
  public final transient ProxyOutputPort<byte[]> error = new ProxyOutputPort<byte[]>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    DelimitedToPojoConverterOperator operator = dag.addOperator("DelimitedToPojoConverter",
        DelimitedToPojoConverterOperator.class);
    operator.setSchemaPath(schemaPath);
    Class<?> schemaClass = null;
    try {
      schemaClass = Class.forName(pojoSchema);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found: " + pojoSchema, e);
    }
    dag.getMeta(operator).getMeta(operator.output).getAttributes().put(Context.PortContext.TUPLE_CLASS, schemaClass);
    input.set(operator.input);
    output.set(operator.output);
    error.set(operator.error);
    dag.getMeta(operator).getAttributes()
        .put(Context.OperatorContext.PARTITIONER, new StatelessPartitioner<>(numberOfPartitions));
    dag.getMeta(operator).getMeta(operator.input).getAttributes().put(Context.PortContext.PARTITION_PARALLEL, true);

  }

  /**
   * Get complete path of schema
   * 
   * @return schemaPath
   */
  public String getSchemaPath()
  {
    return schemaPath;
  }

  /**
   * Set the schemaPath. Schema is a json file residing in HDFS. Complete path
   * of the schema needs to be specified.
   * 
   * @param schemaPath
   */
  public void setSchemaPath(String schemaPath)
  {
    this.schemaPath = schemaPath;
  }

  /**
   * Gets the complete class name of POJO
   * @return completed class name of POJO
   */
  public String getPojoSchema()
  {
    return pojoSchema;
  }

  /**
   * Sets the complete class name of POJO
   * @param pojoSchema complete class name of POJO
   */
  public void setPojoSchema(String pojoSchema)
  {
    this.pojoSchema = pojoSchema;
  }

  public int getNumberOfPartitions()
  {
    return numberOfPartitions;
  }

  /**
   * Specify the number of partitions. Default is 1.
   * 
   * @param numberOfPartitions
   */
  public void setNumberOfPartitions(int numberOfPartitions)
  {
    this.numberOfPartitions = numberOfPartitions;
  }

}
