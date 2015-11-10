/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedparser;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.parser.DelimitedParserOperator;
import com.datatorrent.contrib.parser.DelimitedSchema;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Implementation of {@link #DelimitedParserModule()}. Dag for this module
 * consists of just a single operator - DelimitedParserOperator
 * 
 * Input Ports {@link #input} - input tuples of type byte[]
 * 
 * Output Ports {@link #parsedData} - input tuples found valid against the
 * schema emitted as Map<String,Object> {@link #pojo} - input tuples found valid
 * against the schema emitted as POJO {@link #error} - invalid input tuples
 * emitted as KeyValPair<String, String> . Key is the tuple, Value contains
 * error message
 * 
 */
public class DelimitedParserModule implements Module
{

  /**
   * Path of the schema- json file as specified in {@link DelimitedSchema}
   */
  @NotNull
  private String schemaPath;
  /**
   * number of partitions
   */
  private int numberOfPartitions = 1;

  /**
   * complete class name of pojo
   */
  private String pojoSchema;

  /**
   * {@link #input} input tuples of type byte[]
   */
  public final transient ProxyInputPort<byte[]> input = new ProxyInputPort<byte[]>();

  /**
   * {@link #parsedData} input tuples found valid against the schema emitted as
   * Map<String,Object>
   */
  public final transient ProxyOutputPort<Map<String, Object>> parsedData = new ProxyOutputPort<Map<String, Object>>();

  /**
   * {@link #error} invalid input tuples emitted as KeyValPair<String, String> .
   * Key is the tuple, Value contains error message
   */
  public final transient ProxyOutputPort<KeyValPair<String, String>> error = new ProxyOutputPort<KeyValPair<String, String>>();

  /**
   * {@link #pojo} validated input tuples emitted as pojo.
   */
  public final transient ProxyOutputPort<Object> pojo = new ProxyOutputPort<Object>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    DelimitedParserOperator delimitedParserOperator = dag.addOperator("DelimitedParserOperator",
        DelimitedParserOperator.class);
    delimitedParserOperator.setSchemaPath(schemaPath);
    input.set(delimitedParserOperator.input);
    parsedData.set(delimitedParserOperator.output);
    pojo.set(delimitedParserOperator.pojo);
    error.set(delimitedParserOperator.error);
    if (pojoSchema != null) {
      Class<?> schemaClass = null;
      try {
        schemaClass = Class.forName(pojoSchema);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Class not found: " + pojoSchema, e);
      }
      dag.getMeta(delimitedParserOperator).getMeta(delimitedParserOperator.pojo).getAttributes()
          .put(Context.PortContext.TUPLE_CLASS, schemaClass);
    }
    dag.getMeta(delimitedParserOperator).getAttributes()
        .put(Context.OperatorContext.PARTITIONER, new StatelessPartitioner<>(numberOfPartitions));
    dag.getMeta(delimitedParserOperator).getMeta(delimitedParserOperator.input).getAttributes()
        .put(Context.PortContext.PARTITION_PARALLEL, true);
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

  /**
   * Gets the complete class name of POJO
   * 
   * @return completed class name of POJO
   */
  public String getPojoSchema()
  {
    return pojoSchema;
  }

  /**
   * Sets the complete class name of POJO
   * 
   * @param pojoSchema
   *          complete class name of POJO
   */
  public void setPojoSchema(String pojoSchema)
  {
    this.pojoSchema = pojoSchema;
  }

}
