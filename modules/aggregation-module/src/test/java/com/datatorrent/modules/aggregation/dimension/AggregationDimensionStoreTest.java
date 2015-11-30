/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation.dimension;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.dimensions.aggregator.AggregatorOTFType;
import com.datatorrent.lib.dimensions.aggregator.AggregatorUtils;
import com.datatorrent.lib.dimensions.aggregator.OTFAggregator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.datatorrent.modules.aggregation.monitor.KeyTracker;

public class AggregationDimensionStoreTest
{
  OperatorContext context = new OperatorContext() {
    
    @Override
    public void setCounters(Object counters)
    {
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
    }
    
    @Override
    public <T> T getValue(Attribute<T> key)
    {
      return null;
    }
    
    @Override
    public AttributeMap getAttributes()
    {
      return null;
    }
    
    @Override
    public int getId()
    {
      return 0;
    }

  };
  
  @Rule
  public TestInfo testMeta = new FSTestWatcher() {
    @Override
    protected void starting(Description descriptor)
    {
      super.starting(descriptor);

      try {
        FileUtils.deleteDirectory(new File(getDir()));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(getDir()));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      super.finished(description);
    }
  };

  private PubSubWebSocketAppDataQuery createPubSubQueryOperator()
  {
    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.setTopic("Query");
    wsQuery.setUri(URI.create("ws://localhost:8000/pubsub"));

    return wsQuery;
  }


  @Test
  public void finalizedDataTest() throws Exception
  {
    final String publisher = "google";
    final String advertiser = "safeway";

    String eventSchemaString = SchemaUtils.jarResourceFileToString("adsGenericEventSchemaTest.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AggregationDimensionStore store = new AggregationDimensionStore();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);
    store.setEmbeddableQueryInfoProvider(createPubSubQueryOperator());
    store.getResultFormatter().setContinuousFormatString("#.00");

    CollectorTestSink<String> sink = new CollectorTestSink<>();
    TestUtils.setSink(store.finalizedData, sink);

    store.setup(context);

    DimensionalConfigurationSchema eventSchema = store.getConfigurationSchema();

    long windowId;
    for (windowId = 1L; windowId <= 5; windowId++) {
      store.beginWindow(windowId);
      Aggregate aeSum = createEventSum(eventSchema, publisher, advertiser, 60000L, TimeBucket.MINUTE, 1 * 10, 1.0 * 1.0, 1.0 * 1000.0);
      store.input.put(aeSum);
      Aggregate aeCount = createEventCount(eventSchema, publisher, advertiser, 60000L, TimeBucket.MINUTE, 1, 1, 1);
      store.input.put(aeCount);
      Aggregate aeMax = createEventMax(eventSchema, publisher, advertiser, 60000L, TimeBucket.MINUTE, 1);
      store.input.put(aeMax);
      store.endWindow();
      store.checkpointed(windowId);
      store.committed(windowId);
    }

    store.beginWindow(windowId);
    GPOMutable key = createKey(eventSchema, publisher, advertiser, 60000L, TimeBucket.MINUTE);
    KeyTracker.FinalizedKey data = new KeyTracker.FinalizedKey(0, key);
    store.finalizedKey.put(data);
    store.endWindow();
    store.checkpointed(windowId);

    Assert.assertEquals(1, sink.collectedTuples.size());
    String finalizedData = sink.collectedTuples.get(0);
    String expectedFinalizedData = "{\"groupByKey\":{\"time\":60000,\"timeBucket\":\"1m\",\"advertiser\":\"safeway\",\"publisher\":\"google\"},\"aggregates\":{\"impressions\":{\"MAX\":1,\"COUNT\":5,\"SUM\":50,\"AVG\":10},\"cost\":{\"COUNT\":5,\"SUM\":5,\"AVG\":1},\"revenue\":{\"COUNT\":5,\"SUM\":5000,\"AVG\":1000}}}";
    Assert.assertTrue(finalizedData.equals(expectedFinalizedData));

    store.teardown();
  }

  private Aggregate createEventCount(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long timestamp, TimeBucket timeBucket, long impressions, long cost, long revenue)
  {
    int schemaID = DimensionsComputationFlexibleSingleSchemaPOJO.DEFAULT_SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(AggregatorIncrementalType.COUNT.name());

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(aggregatorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, timeBucket.roundDown(timestamp));
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucket.ordinal());

    EventKey eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorID, key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);
    value.setField("revenue", revenue);

    // Aggregate Event
    return new Aggregate(eventKey, value);
  }

  private Aggregate createEventSum(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long timestamp, TimeBucket timeBucket, long impressions, double cost, double revenue)
  {
    int schemaID = DimensionsComputationFlexibleSingleSchemaPOJO.DEFAULT_SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(AggregatorIncrementalType.SUM.name());

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(aggregatorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, timeBucket.roundDown(timestamp));
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucket.ordinal());

    EventKey eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorID, key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);
    value.setField("revenue", revenue);

    // Aggregate Event
    return new Aggregate(eventKey, value);
  }

  private Aggregate createEventMax(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long timestamp, TimeBucket timeBucket, long impressions)
  {
    int schemaID = DimensionsComputationFlexibleSingleSchemaPOJO.DEFAULT_SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(AggregatorIncrementalType.MAX.name());

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(aggregatorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, timeBucket.roundDown(timestamp));
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucket.ordinal());

    EventKey eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorID, key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);

    // Aggregate Event
    return new Aggregate(eventKey, value);
  }
  
  private Aggregate createEventAvg(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long timestamp, TimeBucket timeBucket, double impressions, double cost, double revenue)
  {
    int schemaID = DimensionsComputationFlexibleSingleSchemaPOJO.DEFAULT_SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = AggregationDimensionStore.staticOTFAggID;

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);
    
    OTFAggregator otfAggregator = eventSchema.getAggregatorRegistry().getNameToOTFAggregators().get(AggregatorOTFType.AVG.name());
    List<String> childAggList = eventSchema.getAggregatorRegistry().getOTFAggregatorToIncrementalAggregators().get(AggregatorOTFType.AVG.name());
    Integer firstChildAggId = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(childAggList.get(0));
    Fields fields = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(firstChildAggId).getFields();
    FieldsDescriptor fdValue = AggregatorUtils.getOutputFieldsDescriptor(fields, otfAggregator);
    
    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, timeBucket.roundDown(timestamp));
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucket.ordinal());

    EventKey eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorID, key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);
    value.setField("revenue", revenue);

    // Aggregate Event
    return new Aggregate(eventKey, value);
  }

  private GPOMutable createKey(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long timestamp, TimeBucket timeBucket)
  {
    int dimensionDescriptorID = 0;

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, timeBucket.roundDown(timestamp));
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucket.ordinal());

    return key;
  }

}
