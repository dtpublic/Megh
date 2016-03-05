/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.util.concurrent.Exchanger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class OrderedBucketManagerTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/OrderedBucketManagerTest";
  private static final long BUCKET_SPAN = 1000;

  private static TestBucketManager<DummyEvent> manager;
  private static String applicationPath;
  private static final Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class TestBucketManager<T extends Event & Bucketable> extends OrderedBucketManagerPOJOImpl
  {
    TestBucketManager()
    {
      super();
    }
  }

  private static class TestStorageManagerListener implements BucketManager.Listener<Object>
  {
    @Override
    public void bucketLoaded(AbstractBucket<Object> bucket)
    {
      try {
        eventBucketExchanger.exchange(bucket.bucketKey);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void bucketOffLoaded(long bucketKey)
    {
    }

    @Override
    public void bucketDeleted(long bucketKey)
    {
    }
  }

  @Test
  public void testExpiration() throws InterruptedException
  {
    DummyEvent event1 = new DummyEvent(1, manager.startOfBuckets + 10 * BUCKET_SPAN);
    long bucket1 = manager.getBucketKeyFor(event1);
    DummyEvent event2 = new DummyEvent(1, manager.startOfBuckets + (manager.noOfBuckets + 10) * BUCKET_SPAN);
    long bucket2 = manager.getBucketKeyFor(event2);
    Assert.assertEquals("bucket index", bucket1 % manager.noOfBuckets, bucket2 % manager.noOfBuckets);

    bucket1 = manager.getBucketKeyFor(event1);
    Assert.assertEquals("expired event", bucket1, -1);

    long rBucket2 = manager.getBucketKeyFor(event2);
    Assert.assertEquals("valid event", bucket2, rBucket2);
  }

  @BeforeClass
  public static void setup() throws Exception
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    manager = new TestBucketManager<DummyEvent>();
    manager.setBucketSpan(BUCKET_SPAN);
    manager.setKeyExpression("id");
    manager.setExpiryExpression("time");
    manager.setExpiryPeriod(10000);
    ExpirableHdfsBucketStore<Object> bucketStore = new ExpirableHdfsBucketStore<Object>();
    manager.setBucketStore(bucketStore);
    bucketStore.setConfiguration(0, applicationPath, Sets.newHashSet(0), 0);
    bucketStore.setup();
    manager.startService(new TestStorageManagerListener());
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    manager.shutdownService();
    Path root = new Path(applicationPath);
    FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
    fs.delete(root, true);
  }
}
