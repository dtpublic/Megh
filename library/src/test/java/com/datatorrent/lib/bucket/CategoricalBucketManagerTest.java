/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

import com.datatorrent.lib.bucket.AbstractCategoricalBucketManager.ExpiryPolicy;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class CategoricalBucketManagerTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/CategoricalBucketManagerTest";
  private static final int NUM_EXPIRY_BUCKETS = 2;

  private static CategoricalBucketManagerPOJOImpl manager;
  private static String applicationPath;

  public static class TestEvent implements Bucketable
  {
    int id;
    long time;
    String fileName;

    public TestEvent(int id, long time, String fileName)
    {
      this.id = id;
      this.time = time;
      this.fileName = fileName;
    }

    @Override
    public Object getEventKey()
    {
      return id;
    }

    public Object getExpiryKey()
    {
      return fileName;
    }

    @Override
    public String toString()
    {
      return id + "." + time + "." + fileName;
    }
  }

  @Test
  public void testExpirationLRU() throws InterruptedException
  {
    // LRU
    manager.setPolicy(ExpiryPolicy.LRU);
    TestEvent event1 = new TestEvent(1, 10, "f1");
    long bucket1 = manager.getBucketKeyFor(event1);
    TestEvent event2 = new TestEvent(2, 20, "f2");
    long bucket2 = manager.getBucketKeyFor(event2);

    TestEvent event3 = new TestEvent(1, 30, "f3");
    long bucket3 = manager.getBucketKeyFor(event3);

    Assert.assertEquals("bucket index", bucket1 % manager.noOfBuckets, bucket3 % manager.noOfBuckets);
    bucket1 = manager.getBucketKeyFor(event1);
    Assert.assertEquals("expired event", bucket1, -1);
    long rBucket2 = manager.getBucketKeyFor(event2);
    Assert.assertEquals("valid event", bucket2, rBucket2);
  }

  @Test
  public void testExpirationFIFO() throws InterruptedException
  {
    // FIFO
    manager.setPolicy(ExpiryPolicy.FIFO);
    TestEvent event1 = new TestEvent(1, 10, "f1");
    long bucket1 = manager.getBucketKeyFor(event1);
    TestEvent event2 = new TestEvent(2, 20, "f2");
    long bucket2 = manager.getBucketKeyFor(event2);

    TestEvent event3 = new TestEvent(1, 30, "f3");
    long bucket3 = manager.getBucketKeyFor(event3);

    Assert.assertEquals("bucket index", bucket1 % manager.noOfBuckets, bucket3 % manager.noOfBuckets);
    bucket1 = manager.getBucketKeyFor(event1);
    Assert.assertEquals("expired event", bucket1, -1);
    long rBucket2 = manager.getBucketKeyFor(event2);
    Assert.assertEquals("valid event", bucket2, rBucket2);
    manager.shutdownService();
  }

  @Before
  public void setup() throws Exception
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    manager = new CategoricalBucketManagerPOJOImpl();
    manager.setExpiryBuckets(NUM_EXPIRY_BUCKETS);
    manager.setKeyExpression("{$}.getEventKey()");
    manager.setExpiryExpression("{$}.getExpiryKey()");
    ExpirableHdfsBucketStore<Object> bucketStore = new ExpirableHdfsBucketStore<Object>();
    manager.setBucketStore(bucketStore);
    bucketStore.setConfiguration(0, applicationPath, Sets.newHashSet(0), 0);
    bucketStore.setup();
    manager.startService(new TestStorageManagerListener());
    manager.setPojoClass(TestEvent.class);
    manager.activate(new OperatorContextTestHelper.TestIdOperatorContext(0, null));
  }

  static class TestStorageManagerListener implements BucketManager.Listener<Object>
  {
    @Override
    public void bucketLoaded(AbstractBucket<Object> bucket)
    {
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

  @After
  public void teardown() throws IOException
  {
    manager.shutdownService();
    Path root = new Path(applicationPath);
    FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
    fs.delete(root, true);
  }
}
