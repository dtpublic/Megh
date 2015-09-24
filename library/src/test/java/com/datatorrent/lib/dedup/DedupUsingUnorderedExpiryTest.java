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
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.OrderedBucketManagerImpl;
import com.datatorrent.lib.bucket.UnorderedBucketManagerImpl;
import com.datatorrent.lib.bucket.AbstractExpirableUnorderedBucketManager.ExpiryPolicy;
import com.datatorrent.lib.bucket.UnorderedBucketManagerTest.TestEvent;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link Deduper}
 */
public class DedupUsingUnorderedExpiryTest
{
  private static final Logger logger = LoggerFactory.getLogger(DedupUsingUnorderedExpiryTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DedupUsingUnorderedExpiryTest";
  private final static String APP_ID = "DedupUsingUnorderedExpiryTest";
  private final static int OPERATOR_ID = 0;

  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperWithHdfsStore<TestEvent, TestEvent>
  {

    @Override
    public void bucketLoaded(AbstractBucket<TestEvent> bucket)
    {
      try{
        super.bucketLoaded(bucket);
        logger.debug("Bucket {} loaded", bucket.bucketKey);
        eventBucketExchanger.exchange(bucket.bucketKey, 1, TimeUnit.MILLISECONDS);
      }
      catch(TimeoutException e){
        logger.debug("Timeout happened");
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public TestEvent convert(TestEvent TestEvent)
    {
      return TestEvent;
    }

    public void addEventManuallyToWaiting(TestEvent event)
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
      events.add(new TestEvent(k, e, "f"+k));
      k++;
      e +=10;
    }

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
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
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
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
    ExpirableHdfsBucketStore<TestEvent>  bucketStore = new ExpirableHdfsBucketStore<TestEvent>();
    deduper = new DummyDeduper();
    UnorderedBucketManagerImpl<TestEvent> storageManager = new UnorderedBucketManagerImpl<TestEvent>();
    storageManager.setExpiryBuckets(5);
    storageManager.setPolicy(ExpiryPolicy.LRU);
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
