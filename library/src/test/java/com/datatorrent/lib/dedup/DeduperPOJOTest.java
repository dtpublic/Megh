/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
import java.util.Date;
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

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.NonOperationalBucketStore;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerPOJOImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.stram.engine.PortContext;

public class DeduperPOJOTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperPOJOTest.class);

  private static final String APPLICATION_PATH_PREFIX = "target/DeduperPOJOTest";
  private static final String APP_ID = "DeduperPOJOTest";
  private static final int OPERATOR_ID = 0;
  private static final Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperPOJOImpl
  {
    @Override
    public void setup(Context.OperatorContext context)
    {
      boolean stateless = context.getValue(Context.OperatorContext.STATELESS);
      if (stateless) {
        bucketManager.setBucketStore(new NonOperationalBucketStore<Object>());
      } else {
        ((HdfsBucketStore<Object>)bucketManager.getBucketStore()).setConfiguration(context.getId(),
            context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
      }
      super.setup(context);
    }

    @Override
    public void bucketLoaded(AbstractBucket<Object> bucket)
    {
      try {
        super.bucketLoaded(bucket);
        eventBucketExchanger.exchange(bucket.bucketKey);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static DummyDeduper deduper;
  private static String applicationPath;

  @Test
  public void testDedup()
  {
    List<InnerObj> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    for (int i = 0; i < 10; i++) {
      events.add(new InnerObj(i, calendar.getTimeInMillis()));
    }
    events.add(new InnerObj(5, calendar.getTimeInMillis()));

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, InnerObj.class);
    OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);
    deduper.setup(context);
    deduper.input.setup(new PortContext(attributes, context));
    deduper.activate(context);
    CollectorTestSink<InnerObj> collectorTestSink = new CollectorTestSink<InnerObj>();
    TestUtils.setSink(deduper.output, collectorTestSink);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 10, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 0");

    logger.debug("start round 1");
    deduper.beginWindow(1);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 0, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 1");

    //Test the sliding window
    try {
      Thread.sleep(1500);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    deduper.handleIdleTime();
    long now = System.currentTimeMillis();
    for (int i = 10; i < 15; i++) {
      events.add(new InnerObj(i, now));
    }

    logger.debug("start round 2");
    deduper.beginWindow(2);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 5, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 2");
    deduper.teardown();
  }

  private void testRound(List<InnerObj> events)
  {
    for (InnerObj event: events) {
      deduper.input.process(event);
    }
    try {
      eventBucketExchanger.exchange(null, 1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      logger.debug("Timeout Happened");
    }
  }

  @BeforeClass
  public static void setup()
  {
    /* seed janino for mvn dependencies */
    Assert.assertNotNull(org.codehaus.janino.util.AutoIndentWriter.class);

    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<Object> bucketStore = new ExpirableHdfsBucketStore<Object>();
    deduper = new DummyDeduper();
    TimeBasedBucketManagerPOJOImpl timeManager = new TimeBasedBucketManagerPOJOImpl();
    timeManager.setKeyExpression("getKey()");
    timeManager.setTimeExpression("getTime()");
    timeManager.setBucketSpan(1200);
    timeManager.setMillisPreventingBucketEviction(1200000);
    timeManager.setBucketStore(bucketStore);
    deduper.setBucketManager(timeManager);
  }

  @AfterClass
  public static void teardown()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private InnerObj innerObj = new InnerObj();

  /**
   * @return the innerObj
   */
  public InnerObj getInnerObj()
  {
    return innerObj;
  }

  /**
   * @param innerObj the innerObj to set
   */
  public void setInnerObj(InnerObj innerObj)
  {
    this.innerObj = innerObj;
  }

  public class InnerObj
  {
    public InnerObj()
    {
    }

    private Date time;
    private int key;

    public int getKey()
    {
      return key;
    }

    public void setKey(int key)
    {
      this.key = key;
    }

    private InnerObj(int i, long timeInMillis)
    {
      time = new Date(timeInMillis);
      key = i;
    }

    public Date getTime()
    {
      return time;
    }

    public void setTime(Date time)
    {
      this.time = time;
    }

  }

}
