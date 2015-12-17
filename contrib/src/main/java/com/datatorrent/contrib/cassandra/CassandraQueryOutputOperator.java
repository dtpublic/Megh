/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.cassandra;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.cassandra.CassandraPOJOOutputOperator;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.PojoUtils;

/**
 * CassandraOutputOperator extends {@link CassandraPOJOOutputOperator} to
 * provide an option <br/>
 * 1. To accept database column to pojo fields mapping as json from user. <br/>
 * The json input contains the mapping from pojo input fields to the database
 * columns. <br/>
 * The json format: <br/>
 * [ { 'input': 'pojo field expression', 'dataType': 'type of data in pojo',
 * 'dbColumnName': 'name of database column' }, { ... } ] <br/>
 * 2. To accept parameterized cql queries from user.<br/>
 * 3. To provide error port where records which couldn't be written to database
 * are emitted. <br/>
 * 4. To expose matric of records processed per second and error records per
 * second.
 * 
 * @category Output
 * @tags database, nosql, pojo, cassandra
 */
@Evolving
public class CassandraQueryOutputOperator extends CassandraPOJOOutputOperator
{
  private static Logger LOG = LoggerFactory.getLogger(CassandraQueryOutputOperator.class);
  private String query;
  private String configJsonString;
  private long errorRecordCount;
  private long recordsCount;
  private double windowTimeSec;
  @AutoMetric
  private long recordsProcessedPerSec;
  @AutoMetric
  private long errorRecordsPerSec;
  public transient DefaultOutputPort<Object> error = new DefaultOutputPort<Object>();

  public CassandraQueryOutputOperator()
  {
    super.setFieldInfos(new ArrayList<FieldInfo>());
  }

  @Override
  public void setup(OperatorContext context)
  {
    List<FieldInfo> fieldInfos = super.getFieldInfos();
    JsonNode configArray;
    // Parse json and identify parameter type and pojo expressions
    try {
      ObjectMapper mapper = new ObjectMapper();
      configArray = mapper.readTree(configJsonString);
      for (int i = 0; i < configArray.size(); i++) {
        JsonNode node = configArray.get(i);
        fieldInfos.add(new FieldInfo(node.get("dbColumnName").asText(), node.get("input").asText(), null));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error loading config json file.");
    }

    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
        * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;

    super.setup(context);
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    //clear if it had anything
    columnDataTypes.clear();
    ResultSet rs = store.getSession().execute("select * from " + store.getKeyspace() + "." + getTablename());
    final ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

    for (FieldInfo fieldInfo : super.getFieldInfos()) {
      final DataType type = rsMetaData.getType(fieldInfo.getColumnName());
      columnDataTypes.add(type);
      final Object getter;
      final String getterExpr = fieldInfo.getPojoFieldExpression();

      switch (type.getName()) {
        case ASCII:
        case TEXT:
        case VARCHAR:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, String.class);
          break;
        case BOOLEAN:
          getter = PojoUtils.createGetterBoolean(pojoClass, getterExpr);
          break;
        case INT:
          getter = PojoUtils.createGetterInt(pojoClass, getterExpr);
          break;
        case BIGINT:
        case COUNTER:
          getter = PojoUtils.createGetterLong(pojoClass, getterExpr);
          break;
        case FLOAT:
          getter = PojoUtils.createGetterFloat(pojoClass, getterExpr);
          break;
        case DOUBLE:
          getter = PojoUtils.createGetterDouble(pojoClass, getterExpr);
          break;
        case DECIMAL:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, BigDecimal.class);
          break;
        case SET:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Set.class);
          break;
        case MAP:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Map.class);
          break;
        case LIST:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, List.class);
          break;
        case TIMESTAMP:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Date.class);
          break;
        case UUID:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, UUID.class);
          break;
        default:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Object.class);
          break;
      }
      getters.add(getter);
    }
  }

  @Override
  protected PreparedStatement getUpdateCommand()
  {
    PreparedStatement statement;
    if (query != null) {
      statement = store.getSession().prepare(query);
    } else {
      statement = super.getUpdateCommand();
    }
    LOG.info("Operator query: " + statement.getQueryString());
    return statement;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    recordsCount = 0;
    errorRecordCount = 0;
    recordsProcessedPerSec = 0;
    errorRecordsPerSec = 0;
  }

  @Override
  public void processTuple(Object tuple)
  {
    try {
      super.processTuple(tuple);
      recordsCount++;
    } catch (RuntimeException e) {
      error.emit(tuple);
      errorRecordCount++;
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    recordsProcessedPerSec = (long)(recordsCount / windowTimeSec);
    errorRecordsPerSec = (long)(errorRecordCount / windowTimeSec);
  }

  public String getQuery()
  {
    return query;
  }

  /**
   * Set cql query which needs to run by operator
   *
   * @param query
   */
  public void setQuery(String query)
  {
    this.query = query;
  }

  public String getConfigJsonString()
  {
    return configJsonString;
  }

  /**
   * configJson which contains mapping from pojo fields to the actual database
   * columns. json format:<br/>
   * [ { 'input': 'pojo field expression', 'dataType': 'type of data in pojo',
   * 'dbColumnName': 'name of database column' }, { ... } ]
   *
   * @param configJsonString
   */
  public void setConfigJsonString(String configJsonString)
  {
    this.configJsonString = configJsonString;
  }
}
