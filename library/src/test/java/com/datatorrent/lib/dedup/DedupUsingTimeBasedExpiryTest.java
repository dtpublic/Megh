/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.dedup;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.DummyEvent;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link Deduper}
 */
public class DedupUsingTimeBasedExpiryTest
{
  private static final Logger logger = LoggerFactory.getLogger(DedupUsingTimeBasedExpiryTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DedupUsingTimeBasedExpiryTest";
  private final static String APP_ID = "DedupUsingTimeBasedExpiryTest";
  private final static int OPERATOR_ID = 0;

  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperWithHdfsStore<DummyEvent, DummyEvent>
  {

    @Override
    public void bucketLoaded(AbstractBucket<DummyEvent> bucket)
    {
      try {
        super.bucketLoaded(bucket);
        eventBucketExchanger.exchange(bucket.bucketKey, 1, TimeUnit.MILLISECONDS);
      }
      catch (Exception e) {
        logger.debug("Timeout happened");
      }
    }

    @Override
    public DummyEvent convert(DummyEvent dummyEvent)
    {
      return dummyEvent;
    }

    public void addEventManuallyToWaiting(DummyEvent event)
    {
      waitingEvents.put(bucketManager.getBucketKeyFor(event), Lists.newArrayList(event));
    }

  }

  private static DummyDeduper deduper;
  private static String applicationPath;

  @Test
  public void testDedup()
  {
    List<DummyEvent> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    int k = 1000;
    int e = 1000;
    long now = calendar.getTimeInMillis();
    for (int i = 0; i < 20; i++) {
      events.add(new DummyEvent(k*1000, e*1000));
      logger.debug("Event: "+k*1000+ " "+e*1000);
      k += 100;
      e += 100;
    }
    k = 1000;
    e = 1000;
    for (int i = 0; i < 10; i++) {
      events.add(new DummyEvent(k*1000, e*1000));
      logger.debug("Event: "+k*1000+ " "+e*1000);
      k += 100;
      e += 100;
    }

    k = 50;
    e = 50;
    for (int i = 0; i < 10; i++) {
      events.add(new DummyEvent(k*1000, e*1000));
      logger.debug("Event: "+k*1000 + " "+e*1000);
      k += 10;
      e += 10;
    }

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
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
    System.out.println(collectorTestSink.collectedTuples);
    System.out.println(collectorTestSinkDup.collectedTuples);
    System.out.println(collectorTestSinkExp.collectedTuples);
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
    try {
      eventBucketExchanger.exchange(null, 1, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    catch (TimeoutException e) {
      logger.debug("Timeout Happened");
    }
  }

  @Test
  public void testDeduperRedeploy() throws Exception
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.addEventManuallyToWaiting(new DummyEvent(2500000, 2500000));
    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attributes));
    eventBucketExchanger.exchange(null, 500, TimeUnit.MILLISECONDS);
    deduper.endWindow();
    deduper.teardown();
  }

  @BeforeClass
  public static void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<DummyEvent>  bucketStore = new ExpirableHdfsBucketStore<DummyEvent>();
    deduper = new DummyDeduper();
    TimeBasedBucketManagerImpl<DummyEvent> storageManager = new TimeBasedBucketManagerImpl<DummyEvent>();
    storageManager.setExpiryPeriod(2000);
    storageManager.setBucketSpan(10);
    storageManager.setUseSystemTime(false);
    storageManager.setMillisPreventingBucketEviction(60000);
    storageManager.setBucketStore(bucketStore);
    deduper.setBucketManager(storageManager);
  }

  @AfterClass
  public static void teardown()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
      logger.debug("Deleted path: "+applicationPath);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
