/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.AbstractExpirableCategoricalBucketManager.ExpiryPolicy;
import com.datatorrent.lib.bucket.CategoricalBucketManagerTest.TestEvent;
import com.datatorrent.lib.bucket.ExpirableCategoricalBucketManagerPOJOImpl;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStoreAsync;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link Deduper}
 */
public class DedupUsingCategoricalExpiryTest
{
  private static final Logger logger = LoggerFactory.getLogger(DedupUsingCategoricalExpiryTest.class);

  private static final String APPLICATION_PATH_PREFIX = "target/DedupUsingUnorderedExpiryTest";
  private static final String APP_ID = "DedupUsingUnorderedExpiryTest";
  private static final int OPERATOR_ID = 0;

  private static final Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperCategoricalPOJOImpl
  {

    @Override
    public void bucketLoaded(AbstractBucket<Object> bucket)
    {
      try {
        super.bucketLoaded(bucket);
        logger.debug("Bucket {} loaded", bucket.bucketKey);
        eventBucketExchanger.exchange(bucket.bucketKey, 1, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        logger.debug("Timeout happened");
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public Object convert(Object TestEvent)
    {
      return TestEvent;
    }

    public void addEventManuallyToWaiting(Object event)
    {
      waitingEvents.put(bucketManager.getBucketKeyFor(event), Lists.newArrayList(event));
    }

  }

  private static DummyDeduper deduper;
  private static String applicationPath;

  @Test
  public void testDedup()
  {
    List<TestEvent> events = Lists.newArrayList();
    int k = 1;
    int e = 10;
    for (int i = 0; i < 5; i++) {
      events.add(new TestEvent(k, e, "f" + k));
      k++;
      e += 10;
    }

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<TestEvent> collectorTestSink = new CollectorTestSink<TestEvent>();
    TestUtils.setSink(deduper.output, collectorTestSink);
    CollectorTestSink<TestEvent> collectorTestSinkDup = new CollectorTestSink<TestEvent>();
    TestUtils.setSink(deduper.duplicates, collectorTestSinkDup);
    CollectorTestSink<TestEvent> collectorTestSinkExp = new CollectorTestSink<TestEvent>();
    TestUtils.setSink(deduper.expired, collectorTestSinkExp);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();

    Assert.assertEquals("Output tuples", 5, collectorTestSink.collectedTuples.size());
    Assert.assertEquals("Duplicate tuples", 0, collectorTestSinkDup.collectedTuples.size());
    Assert.assertEquals("Expired tuples", 0, collectorTestSinkExp.collectedTuples.size());
    collectorTestSink.clear();
    collectorTestSinkDup.clear();
    collectorTestSinkExp.clear();
    logger.debug("end round 0");
    deduper.teardown();
  }

  private void testRound(List<TestEvent> events)
  {
    for (TestEvent event : events) {
      deduper.input.process(event);
    }
  }

  //@Test
  public void testDeduperRedeploy() throws Exception
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.addEventManuallyToWaiting(new TestEvent(6, 60, "f5"));
    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attributes));
    eventBucketExchanger.exchange(null, 500, TimeUnit.MILLISECONDS);
    deduper.endWindow();
    deduper.teardown();
  }

  @BeforeClass
  public static void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStoreAsync<Object>  bucketStore = new ExpirableHdfsBucketStoreAsync<Object>();
    deduper = new DummyDeduper();
    ExpirableCategoricalBucketManagerPOJOImpl storageManager = new ExpirableCategoricalBucketManagerPOJOImpl();
    storageManager.setExpiryBuckets(5);
    storageManager.setPolicy(ExpiryPolicy.LRU);
    storageManager.setMillisPreventingBucketEviction(60000);
    storageManager.setBucketStore(bucketStore);
    storageManager.setKeyExpression("{$}.getEventKey()");
    storageManager.setExpiryExpression("{$}.getExpiryKey()");
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
