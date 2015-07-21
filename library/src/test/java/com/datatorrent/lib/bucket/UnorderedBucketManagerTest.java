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
package com.datatorrent.lib.bucket;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.datatorrent.lib.bucket.AbstractExpirableUnorderedBucketManager.ExpiryPolicy;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class UnorderedBucketManagerTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/UnorderedBucketManagerTest";
  private static final int NUM_EXPIRY_BUCKETS = 2;

  private static TestBucketManager<TestEvent> manager;
  private static String applicationPath;

  public static class TestEvent implements Expirable, Bucketable
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

    @Override
    public Object getExpiryKey()
    {
      return fileName;
    }

    @Override
    public String toString(){
      return id+"."+time+"."+fileName;
    }
  }

  private static class TestBucketManager<T extends Expirable & Bucketable> extends UnorderedBucketManagerImpl<TestEvent>
  {
    TestBucketManager()
    {
      super();
    }
  }

  @Test
  public void testExpirationLRU() throws InterruptedException
  {
    //LRU
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
    //FIFO
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

  @Test
  public void testClone() throws CloneNotSupportedException, InterruptedException
  {
    AbstractExpirableUnorderedBucketManager<TestEvent> clonedManager = manager.clone();
    Assert.assertNotNull(clonedManager);
    Assert.assertNotNull(clonedManager.getBucketStore());
    Assert.assertTrue(clonedManager.bucketStore.equals(manager.bucketStore));
    Assert.assertTrue(clonedManager.writeEventKeysOnly==manager.writeEventKeysOnly);
    Assert.assertTrue(clonedManager.noOfBuckets==manager.noOfBuckets);
    Assert.assertTrue(clonedManager.noOfBucketsInMemory==manager.noOfBucketsInMemory);
    Assert.assertTrue(clonedManager.maxNoOfBucketsInMemory==manager.maxNoOfBucketsInMemory);
    Assert.assertTrue(clonedManager.millisPreventingBucketEviction== manager.millisPreventingBucketEviction);
    Assert.assertTrue(clonedManager.committedWindow==manager.committedWindow);
  }

  @Before
  public void setup() throws Exception
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    manager = new TestBucketManager<TestEvent>();
    manager.setExpiryBuckets(NUM_EXPIRY_BUCKETS);
    ExpirableHdfsBucketStore<TestEvent> bucketStore = new ExpirableHdfsBucketStore<TestEvent>();
    manager.setBucketStore(bucketStore);
    bucketStore.setConfiguration(0, applicationPath, Sets.newHashSet(0), 0);
    bucketStore.setup();
    manager.startService(new TestStorageManagerListener());
  }

  static class TestStorageManagerListener implements BucketManager.Listener<TestEvent>
  {
    @Override
    public void bucketLoaded(AbstractBucket<TestEvent> bucket)
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

  @AfterClass
  public static void teardown() throws IOException
  {
    manager.shutdownService();
    Path root = new Path(applicationPath);
    FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
    fs.delete(root, true);
  }
}