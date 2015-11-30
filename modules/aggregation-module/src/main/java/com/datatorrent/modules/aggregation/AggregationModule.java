/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.hdht.HDHTFileAccess;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import com.datatorrent.modules.aggregation.dimension.AggregationDimensionStore;
import com.datatorrent.modules.aggregation.monitor.KeyTracker;
import com.datatorrent.modules.aggregation.schema.DimensionalSchemaDesc;
import com.datatorrent.modules.aggregation.schema.DimensionalSchemaDescGenerator;

public class AggregationModule implements Module
{
  private static final String MODULE_NAME = "AggregationModule";
  public static final String DEFAULT_TIME_BUCKETS = DimensionalSchemaDesc.ONE_MINUTE + "," + DimensionalSchemaDesc.ONE_HOUR + "," + DimensionalSchemaDesc.ONE_DAY;
  public static final String DEFAULT_FINALIZATION_POLICY = "TIMEBASED";
  public static final int DEFAULT_AGGREGATION_INTERVAL = 60;
  public static final String PUBSUB_TOPIC_NAME_PREFIX = MODULE_NAME + "." + System.currentTimeMillis() + ".";

  /**
   * Qualified class path of POJO class which will be arriving at the input port of
   * Aggregation Module.
   */
  @NotNull
  private String pojoSchema;

  /**
   * Path to the JSON based Dimensions Schema Configuration file.
   * This is an optional parameter if groupByKeys, computeFields & timeBuckets parameters are not mentioned.
   */
  private String computationSchemaFilePath;

  /**
   * All the combinations for keys which will be used for grouping.
   * The combinations should be separated by a semicolon (;) and each key is in a combination should be
   * separated by a comma (,).
   * For eg. field1;field2;field1,field2
   *
   * If computationSchemaFilePath parameter will override this parameter.
   */
  private String groupByKeys;

  /**
   * List of fields on which aggregate should be computed and
   * also the aggregate function to be applied on them. Each definition will be seperated by a semicolon (;).
   * The field name and the aggregate function should be seperated by colon (:).
   * Each aggregate function should be seperated by a comma(,).
   * For eg. value1:SUM,COUNT,AVG;value2:SUM,AVG,MIN
   */
  private String computeFields;

  /**
   * Comma separated list of timeBuckets over which aggregation will be done.
   *
   * Default is "1m,1h,1d"
   */
  private String timeBuckets;

  /**
   * Name of the field in the input POJO containing the time field
   */
  @NotNull
  private String timeFieldName;

  /**
   * Finalization policy of aggregated values. Possible values are TIMEBASED, CATEGORICAL
   * Currently only TIMEBSAED is supported.
   *
   * Default is TIMEBASED.
   */
  private String finalizationPolicy = DEFAULT_FINALIZATION_POLICY;

  /**
   * Time interval in sec after which an aggregate will be treated as finalized OR not going to change.
   * This parameter will take effect only if finalizationPolicy is set to TIMEBASED.
   *
   * Default in 120 sec.
   */
  private int activeAggregationInterval;

  /**
   * Whether to delete the data from store after key is finalized.
   *
   * Default is false.
   */
  private boolean deleteFinalizedData = false;

  /* Attributes exposed as module properties */

  /**
   * Number of partitions for DimensionsComputation operator.
   * The partitioner set will be com.datatorrent.common.partitioner.StatelessPartitioner.
   *
   * Default is no partitioning.
   */
  private int dimensionComputationPartitionCount = 1;

  /**
   * Whether DimensionsComputation operator is in parallel partition with upstream operator.
   *
   * Default is false.
   */
  private boolean dimensionComputationInParallel = false;

  /**
   * Memory requirement for Dimensions Computation in MB
   *
   * Default is 1024 MB.
   */
  private int dimensionComputationMemoryMB = Context.OperatorContext.MEMORY_MB.defaultValue;

  /**
   * This defines number of partitions for DimensionsStore operator.
   * The partitioner set will be com.datatorrent.common.partitioner.StatelessPartitioner.
   *
   * Default is no partitioning.
   */
  private int storePartitionCount = 1;

  /**
   * Memory requirement for Dimensions Store in MB
   *
   * Default is 1024 MB.
   */
  private int storeMemoryMB = Context.OperatorContext.MEMORY_MB.defaultValue;

  /**
   * This port accepts the input into Aggregation Module and starts processing it.
   *
   * The POJO incoming into this port should be of type as defined in pojoSchema.
   */
  public final transient ProxyInputPort<Object> inputPOJO = new ProxyInputPort<>();

  /**
   * This port outputs JSON based finalized data as per configured policy constraints.
   */
  public final transient ProxyOutputPort<String> finalizedData = new ProxyOutputPort<>();

  /* Private variables internal to module */
  private String computationalSchema;
  private Map<String, String> expressionMap;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    try {
      initialize();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }


    /* Adding Dimensions Computation operator */
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);
    /* Setting dimensions computation operator properties */
    dimensions.setKeyToExpression(expressionMap);
    dimensions.setAggregateToExpression(expressionMap);
    dimensions.setConfigurationSchemaJSON(computationalSchema);
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<DimensionsEvent.InputEvent, DimensionsEvent.Aggregate>());

    /* Setting dimensions computation operator attributes */
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.MEMORY_MB, dimensionComputationMemoryMB);
    dag.getMeta(dimensions).getMeta(dimensions.input).getAttributes().put(Context.PortContext.PARTITION_PARALLEL, dimensionComputationInParallel);
    if (!dimensionComputationInParallel) {
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.PARTITIONER, new StatelessPartitioner<>(dimensionComputationPartitionCount));
    }
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);


    /* Adding tracker */
    KeyTracker tracker = dag.addOperator("KeyTracker", KeyTracker.class);
    /* Setting tracker properties */
    tracker.setConfigurationSchemaJSON(computationalSchema);
    tracker.setActiveAggregationInterval(activeAggregationInterval == 0 ? DEFAULT_AGGREGATION_INTERVAL : activeAggregationInterval);
    tracker.setFinalizationPolicy(KeyTracker.FinalizationPolicy.valueOf(finalizationPolicy));
    /* Setting tracker attributes */
    dag.getMeta(tracker).getAttributes().put(Context.OperatorContext.MEMORY_MB, storeMemoryMB);
    dag.getMeta(tracker).getAttributes().put(Context.OperatorContext.PARTITIONER, new StatelessPartitioner<>(storePartitionCount));


    /* Adding Aggregation Dimension Store */
    AggregationDimensionStore store = dag.addOperator("Store", AggregationDimensionStore.class);
    /* Setting store properties */
    store.setFileStore(createHDHTStoreFile());
    store.getResultFormatter().setContinuousFormatString("#.00");
    store.setConfigurationSchemaJSON(computationalSchema);
    store.setDeleteFinalizedData(deleteFinalizedData);
    store.setEmbeddableQueryInfoProvider(createPubSubQueryOperator(dag));
    /* Setting store attributes */
    dag.getMeta(store).getAttributes().put(Context.OperatorContext.MEMORY_MB, storeMemoryMB);
    dag.getMeta(store).getMeta(store.input).getAttributes().put(Context.PortContext.PARTITION_PARALLEL, true);
    dag.getMeta(store).getMeta(store.finalizedKey).getAttributes().put(Context.PortContext.PARTITION_PARALLEL, true);


    /* Adding pubsub query result operator */
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsOut.setUri(createGatewayURI(dag));
    wsOut.setTopic(PUBSUB_TOPIC_NAME_PREFIX + "QueryResult");
    wsOut.setNumRetries(2147483647);


    /* Add streams */
    inputPOJO.set(dimensions.input);
    dag.addStream("PartialAggregates", dimensions.output, tracker.input);
    dag.addStream("ForwardedPartialAggregates", tracker.partialAggregates, store.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("FinalizedKeys", tracker.finalizedKey, store.finalizedKey).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("QueryResults", store.queryResult, wsOut.input);
    finalizedData.set(store.finalizedData);
  }

  private URI createGatewayURI(DAG dag)
  {
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    String url;
    if (gatewayAddress == null) {
      url = "ws://localhost:9090/pubsub";
    } else {
      url = "ws://" + gatewayAddress + "/pubsub";
    }

    return URI.create(url);
  }

  private PubSubWebSocketAppDataQuery createPubSubQueryOperator(DAG dag)
  {
    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.setTopic(PUBSUB_TOPIC_NAME_PREFIX + "Query");
    wsQuery.setUri(createGatewayURI(dag));

    return wsQuery;
  }

  private HDHTFileAccess createHDHTStoreFile()
  {
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    String basePath = "AggregationModule" + Path.SEPARATOR + "Store" + Path.SEPARATOR + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);

    return hdsFile;
  }

  private void initialize() throws NoSuchFieldException, IOException
  {
    DimensionalSchemaDescGenerator generator = new DimensionalSchemaDescGenerator();
    if (computationSchemaFilePath == null) {
      if (timeBuckets == null) {
        timeBuckets = DEFAULT_TIME_BUCKETS;
      }
      computationalSchema = generator.generateSchema(groupByKeys, computeFields, timeBuckets, pojoSchema);
    } else {
      InputStream in = new FileInputStream(computationSchemaFilePath);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(in, out);
      out.close();
      computationalSchema = new String(out.toByteArray());
      generator.setSchema(computationalSchema);
    }

    expressionMap = generator.generateGetters(timeFieldName, pojoSchema);
  }

  public String getPojoSchema()
  {
    return pojoSchema;
  }

  public void setPojoSchema(String pojoSchema)
  {
    this.pojoSchema = pojoSchema;
  }

  public String getGroupByKeys()
  {
    return groupByKeys;
  }

  public void setGroupByKeys(String groupByKeys)
  {
    this.groupByKeys = groupByKeys;
  }

  public String getComputeFields()
  {
    return computeFields;
  }

  public void setComputeFields(String computeFields)
  {
    this.computeFields = computeFields;
  }

  public String getTimeFieldName()
  {
    return timeFieldName;
  }

  public void setTimeFieldName(String timeFieldName)
  {
    this.timeFieldName = timeFieldName;
  }

  public String getTimeBuckets()
  {
    return timeBuckets;
  }

  public void setTimeBuckets(String timeBuckets)
  {
    this.timeBuckets = timeBuckets;
  }

  public String getFinalizationPolicy()
  {
    return finalizationPolicy;
  }

  public void setFinalizationPolicy(String finalizationPolicy)
  {
    this.finalizationPolicy = finalizationPolicy;
  }

  public int getActiveAggregationInterval()
  {
    return activeAggregationInterval;
  }

  public void setActiveAggregationInterval(int activeAggregationInterval)
  {
    this.activeAggregationInterval = activeAggregationInterval;
  }

  public boolean isDeleteFinalizedData()
  {
    return deleteFinalizedData;
  }

  public void setDeleteFinalizedData(boolean deleteFinalizedData)
  {
    this.deleteFinalizedData = deleteFinalizedData;
  }

  public String getComputationalSchema()
  {
    return computationalSchema;
  }

  public void setComputationalSchema(String computationalSchema)
  {
    this.computationalSchema = computationalSchema;
  }

  public int getDimensionComputationPartitionCount()
  {
    return dimensionComputationPartitionCount;
  }

  public void setDimensionComputationPartitionCount(int dimensionComputationPartitionCount)
  {
    this.dimensionComputationPartitionCount = dimensionComputationPartitionCount;
  }

  public boolean isDimensionComputationInParallel()
  {
    return dimensionComputationInParallel;
  }

  public void setDimensionComputationInParallel(boolean dimensionComputationInParallel)
  {
    this.dimensionComputationInParallel = dimensionComputationInParallel;
  }

  public int getDimensionComputationMemoryMB()
  {
    return dimensionComputationMemoryMB;
  }

  public void setDimensionComputationMemoryMB(int dimensionComputationMemoryMB)
  {
    this.dimensionComputationMemoryMB = dimensionComputationMemoryMB;
  }

  public String getComputationSchemaFilePath()
  {
    return computationSchemaFilePath;
  }

  public void setComputationSchemaFilePath(String computationSchemaFilePath)
  {
    this.computationSchemaFilePath = computationSchemaFilePath;
  }

  public int getStorePartitionCount()
  {
    return storePartitionCount;
  }

  public void setStorePartitionCount(int storePartitionCount)
  {
    this.storePartitionCount = storePartitionCount;
  }

  public int getStoreMemoryMB()
  {
    return storeMemoryMB;
  }

  public void setStoreMemoryMB(int storeMemoryMB)
  {
    this.storeMemoryMB = storeMemoryMB;
  }
}
