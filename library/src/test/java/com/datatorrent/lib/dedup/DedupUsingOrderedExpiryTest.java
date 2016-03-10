/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.DummyEvent;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.OrderedBucketManagerPOJOImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.stram.engine.PortContext;

/**
 * Tests for Deduper using {@link OrderedBucketManagerPOJOImpl}
 */
public class DedupUsingOrderedExpiryTest
{
  private static final Logger logger = LoggerFactory.getLogger(DedupUsingOrderedExpiryTest.class);

  private static final String APPLICATION_PATH_PREFIX = "target/DedupUsingOrderedExpiryTest";
  private static final String APP_ID = "DedupUsingOrderedExpiryTest";
  private static final int OPERATOR_ID = 0;

  private static final Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduperOrderedPOJOImpl extends AbstractBloomFilterDeduper<Object, Object>
  {

    private transient Getter<Object, Object> getter;

    @Override
    public void setup(OperatorContext context)
    {
      ((ExpirableHdfsBucketStore<Object>)bucketManager.getBucketStore()).setConfiguration(context.getId(),
          context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
      super.setup(context);
    }

    @Override
    public void activate(Context context)
    {
      super.activate(context);
      Preconditions.checkArgument(getPojoClass() != null);
      getter = PojoUtils.createGetter(getPojoClass(),
              ((OrderedBucketManagerPOJOImpl)bucketManager).getKeyExpression(), Object.class);
    }

    @Override
    protected Object convert(Object event)
    {
      return event;
    }

    public void setBucketManager(@NotNull BucketManager<Object> bucketManager)
    {
      this.bucketManager = Preconditions.checkNotNull(bucketManager, "storage manager");
      super.setBucketManager(bucketManager);
    }

    @Override
    public BucketManager<Object> getBucketManager()
    {
      return (BucketManager<Object>)bucketManager;
    }

    @Override
    protected Object getEventKey(Object event)
    {
      return getter.get(event);
    }

    @Override
    public void bucketLoaded(AbstractBucket<Object> bucket)
    {
      try {
        super.bucketLoaded(bucket);
        eventBucketExchanger.exchange(bucket.bucketKey, 1, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        logger.debug("Timeout happened");
      }
    }

    public void addEventManuallyToWaiting(Object event)
    {
      waitingEvents.put(bucketManager.getBucketKeyFor(event), Lists.newArrayList(event));
    }

  }

  private static DummyDeduperOrderedPOJOImpl deduper;
  private static String applicationPath;

  @Test
  public void testDedup()
  {
    List<DummyEvent> events = Lists.newArrayList();
    int k = 1000;
    int e = 1000;
    for (int i = 0; i < 20; i++) {
      events.add(new DummyEvent(k, e));
      logger.debug("Event: " + k + " " + e);
      k += 10;
      e += 10;
    }
    k = 1050;
    e = 1050;
    for (int i = 0; i < 10; i++) {
      events.add(new DummyEvent(k, e));
      logger.debug("Event: " + k + " " + e);
      k += 10;
      e += 10;
    }

    k = 50;
    e = 50;
    for (int i = 0; i < 10; i++) {
      events.add(new DummyEvent(k, e));
      logger.debug("Event: " + k + " " + e);
      k += 10;
      e += 10;
    }

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, DummyEvent.class);

    OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);
    deduper.setup(context);
    deduper.input.setup(new PortContext(attributes, context));
    deduper.activate(context);
    CollectorTestSink<DummyEvent> collectorTestSink = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.output, collectorTestSink);
    CollectorTestSink<DummyEvent> collectorTestSinkDup = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.duplicates, collectorTestSinkDup);
    CollectorTestSink<DummyEvent> collectorTestSinkExp = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.expired, collectorTestSinkExp);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("Output tuples", 20, collectorTestSink.collectedTuples.size());
    Assert.assertEquals("Duplicate tuples", 10, collectorTestSinkDup.collectedTuples.size());
    Assert.assertEquals("Expired tuples", 10, collectorTestSinkExp.collectedTuples.size());
    collectorTestSink.clear();
    collectorTestSinkDup.clear();
    collectorTestSinkExp.clear();
    logger.debug("end round 0");
    deduper.teardown();
  }

  private void testRound(List<DummyEvent> events)
  {
    for (DummyEvent event : events) {
      deduper.input.process(event);
    }
  }

  @Test
  public void testDeduperRedeploy() throws Exception
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.addEventManuallyToWaiting(new DummyEvent(100, System.currentTimeMillis()));
    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attributes));
    eventBucketExchanger.exchange(null, 500, TimeUnit.MILLISECONDS);
    deduper.endWindow();
    deduper.teardown();
  }

  @BeforeClass
  public static void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<Object> bucketStore = new ExpirableHdfsBucketStore<Object>();
    deduper = new DummyDeduperOrderedPOJOImpl();
    OrderedBucketManagerPOJOImpl storageManager = new OrderedBucketManagerPOJOImpl();
    storageManager.setExpiryPeriod(1000);
    storageManager.setBucketSpan(10);
    storageManager.setMillisPreventingBucketEviction(60000);
    storageManager.setBucketStore(bucketStore);
    storageManager.setKeyExpression("{$}.getEventKey()");
    storageManager.setExpiryExpression("{$}.getTime()");
    deduper.setBucketManager(storageManager);
  }

  @AfterClass
  public static void teardown()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
      logger.debug("Deleted path: " + applicationPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
