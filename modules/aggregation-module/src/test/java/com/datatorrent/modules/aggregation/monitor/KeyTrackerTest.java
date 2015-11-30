/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.aggregation.monitor;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;

public class KeyTrackerTest
{
  private final String publisher = "google";
  private final String advertiser = "safeway";

  private KeyTracker operator;
  CollectorTestSink<KeyTracker.FinalizedKey> sink;
  DimensionalConfigurationSchema eventSchema;

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

      String eventSchemaString = SchemaUtils.jarResourceFileToString("adsGenericEventSchemaTest.json");

      operator = new KeyTracker();
      operator.setConfigurationSchemaJSON(eventSchemaString);
      operator.setActiveAggregationInterval(60);

      sink = new CollectorTestSink<>();
      TestUtils.setSink(operator.finalizedKey, sink);
      operator.setup(null);

      eventSchema = operator.configurationSchema;
    }

    @Override
    protected void finished(Description description)
    {
      operator.teardown();

      try {
        FileUtils.deleteDirectory(new File(getDir()));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      super.finished(description);
    }
  };

  @Test
  public void timeBasedDatFinalizationTestSameKeyMultipleTimes()
  {
    long currentTime = TimeBucket.MINUTE.roundDown(System.currentTimeMillis()) - 2 * TimeBucket.MINUTE.getTimeUnit().toMillis(1);

    long windowId = 1L;
    operator.beginWindow(windowId);
    for (int i = 0; i < 4; i++) {
      Aggregate sum = createEventSumTimeBased(eventSchema, publisher, advertiser, currentTime, TimeBucket.MINUTE, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();

    KeyTracker.FinalizedKey expectedKey = new KeyTracker.FinalizedKey(0, createKeyTimeBased(eventSchema, publisher, advertiser, currentTime, TimeBucket.MINUTE));

    Assert.assertEquals(expectedKey, sink.collectedTuples.get(0));
  }

  @Test
  public void timeBasedDatFinalizationTestMultipleKeysSameBucket()
  {
    long currentTime = TimeBucket.MINUTE.roundDown(System.currentTimeMillis()) - 2 * TimeBucket.MINUTE.getTimeUnit().toMillis(1);

    long windowId = 1L;
    operator.beginWindow(windowId);
    for (int i = 0; i < 4; i++) {
      Aggregate sum = createEventSumTimeBased(eventSchema, publisher + i, advertiser + i, currentTime, TimeBucket.MINUTE, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();

    Assert.assertEquals(4, sink.collectedTuples.size());

    for (int i = 0; i < 4; i++) {
      KeyTracker.FinalizedKey expectedKey = new KeyTracker.FinalizedKey(0, createKeyTimeBased(eventSchema, publisher + i, advertiser + i, currentTime, TimeBucket.MINUTE));
      Assert.assertEquals(expectedKey, sink.collectedTuples.get(i));
    }
  }

  @Test
  public void timeBasedDatFinalizationTestReemitSameKey()
  {
    long currentTime = TimeBucket.MINUTE.roundDown(System.currentTimeMillis()) - 2 * TimeBucket.MINUTE.getTimeUnit().toMillis(1);

    long windowId = 1L;
    operator.beginWindow(windowId);
    for (int i = 0; i < 4; i++) {
      Aggregate sum = createEventSumTimeBased(eventSchema, publisher, advertiser, currentTime, TimeBucket.MINUTE, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();

    KeyTracker.FinalizedKey expectedKey = new KeyTracker.FinalizedKey(0, createKeyTimeBased(eventSchema, publisher, advertiser, currentTime, TimeBucket.MINUTE));

    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals(expectedKey, sink.collectedTuples.get(0));

    sink.clear();

    windowId++;
    operator.beginWindow(windowId);
    for (int i = 0; i < 4; i++) {
      Aggregate sum = createEventSumTimeBased(eventSchema, publisher, advertiser, currentTime, TimeBucket.MINUTE, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();
    Assert.assertEquals(0, sink.collectedTuples.size());
    operator.endWindow();
    Assert.assertEquals(0, sink.collectedTuples.size());
  }

  @Test
  public void timeBasedDatFinalizationTestMultipleKeysMultipleBuckets()
  {
    long windowId = 1L;
    operator.beginWindow(windowId);
    for (int i = 0; i < 4; i++) {
      long currentTime = TimeBucket.MINUTE.roundDown(System.currentTimeMillis()) - 2 * TimeBucket.MINUTE.getTimeUnit().toMillis(i + 1);
      Aggregate sum = createEventSumTimeBased(eventSchema, publisher + i, advertiser + i, currentTime, TimeBucket.MINUTE, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();

    Assert.assertEquals(4, sink.collectedTuples.size());

    for (int i = 0; i < 4; i++) {
      long currentTime = TimeBucket.MINUTE.roundDown(System.currentTimeMillis()) - 2 * TimeBucket.MINUTE.getTimeUnit().toMillis(i + 1);
      GPOMutable expectedKey = createKeyTimeBased(eventSchema, publisher + i, advertiser + i, currentTime, TimeBucket.MINUTE);
      KeyTracker.FinalizedKey expectedData = new KeyTracker.FinalizedKey(0, expectedKey);
      Assert.assertEquals(expectedData, sink.collectedTuples.get(i));
    }
  }

  @Test
  public void nonTimeBasedDatFinalizationTestMultipleKeys()
  {
    operator.setMaxAggregateStored(5);
    operator.setFinalizationPolicy(KeyTracker.FinalizationPolicy.CATEGORICAL);
    operator.setup(null);

    long windowId = 1L;
    operator.beginWindow(windowId);
    for (int i = 0; i < 5; i++) {
      Aggregate sum = createEventSumNonTimeBased(eventSchema, publisher + i, advertiser + i, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();
    Assert.assertEquals(0, sink.collectedTuples.size());
    operator.endWindow();
    Assert.assertEquals(0, sink.collectedTuples.size());

    windowId++;
    operator.beginWindow(windowId);
    for (int i = 5; i < 10; i++) {
      Aggregate sum = createEventSumNonTimeBased(eventSchema, publisher + i, advertiser + i, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();

    Assert.assertEquals(5, sink.collectedTuples.size());

    for (int i = 0; i < 5; i++) {
      GPOMutable expectedKey = createKeyNonTimeBased(eventSchema, publisher + i, advertiser + i);
      Assert.assertEquals(new KeyTracker.FinalizedKey(0, expectedKey), sink.collectedTuples.get(i));
    }

    sink.clear();

    windowId++;
    int i = 10;
    operator.beginWindow(windowId);
    Aggregate sum = createEventSumNonTimeBased(eventSchema, publisher + i, advertiser + i, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
    operator.input.put(sum);
    operator.endWindow();
    Assert.assertEquals(1, sink.collectedTuples.size());
    i = 5;
    GPOMutable expectedKey = createKeyNonTimeBased(eventSchema, publisher + i, advertiser + i);
    Assert.assertEquals(new KeyTracker.FinalizedKey(0, expectedKey), sink.collectedTuples.get(0));

    sink.clear();
  }

  @Test
  public void nonTimeBasedDatFinalizationTestSameKeyMultipleTimes()
  {
    operator.setMaxAggregateStored(5);
    operator.setFinalizationPolicy(KeyTracker.FinalizationPolicy.CATEGORICAL);
    operator.setup(null);

    long windowId = 1L;
    operator.beginWindow(windowId);
    for (int i = 0; i < 4; i++) {
      Aggregate sum = createEventSumNonTimeBased(eventSchema, publisher, advertiser, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();
    Assert.assertEquals(0, sink.collectedTuples.size());
    operator.endWindow();
    Assert.assertEquals(0, sink.collectedTuples.size());

    windowId++;
    operator.beginWindow(windowId);
    for (int i = 1; i <= 5; i++) {
      Aggregate sum = createEventSumNonTimeBased(eventSchema, publisher + i, advertiser + i, 10 + i, 5.0 + i * 1.0, 1000.0 + i * 1000.0);
      operator.input.put(sum);
    }
    operator.endWindow();
    Assert.assertEquals(1, sink.collectedTuples.size());

    GPOMutable expectedKey = createKeyNonTimeBased(eventSchema, publisher, advertiser);
    Assert.assertEquals(new KeyTracker.FinalizedKey(0, expectedKey), sink.collectedTuples.get(0));
  }

  private Aggregate createEventSumTimeBased(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long timestamp, TimeBucket timeBucket, long impressions, double cost, double revenue)
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

  private Aggregate createEventSumNonTimeBased(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long impressions, double cost, double revenue)
  {
    int schemaID = DimensionsComputationFlexibleSingleSchemaPOJO.DEFAULT_SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(AggregatorIncrementalType.SUM.name());

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(aggregatorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);

    EventKey eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorID, key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);
    value.setField("revenue", revenue);

    // Aggregate Event
    return new Aggregate(eventKey, value);
  }

  private GPOMutable createKeyTimeBased(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser, long timestamp, TimeBucket timeBucket)
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

  private GPOMutable createKeyNonTimeBased(DimensionalConfigurationSchema eventSchema, String publisher, String advertiser)
  {
    int dimensionDescriptorID = 0;

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);

    return key;
  }

}
