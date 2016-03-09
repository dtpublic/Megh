package com.datatorrent.contrib.hdht;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;


import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.partitioner.StatelessPartitionerTest.PartitioningContextImpl;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.Slice;

public class HDHTDynamicPartitioningTest
{
  private static final Logger logger = LoggerFactory.getLogger(HDHTDynamicPartitioningTest.class);

  static Slice getLongByteArray(long key)
  {
    ByteBuffer bb = ByteBuffer.allocate(8);
    bb.putLong(key);
    return new Slice(bb.array());
  }

  public long getLong(byte[] value) throws IOException
  {
    ByteBuffer bb = ByteBuffer.wrap(value);
    return bb.getLong();
  }

  /**
   * Test for testing dynamic partitioning. Check that recovery from previous
   * WAL is performed as expected when number of partitions is increased Create
   * a single partition, put values for 2 buckets Split operator in two
   * operators: 1 bucket per partition Run recovery and check that committed and
   * uncommitted WAL values are returned appropriately after partitioning In
   * this test, the new partitions have a single parent WAL
   */
  @Test
  public void testPartitioningStateTransfer() throws Exception
  {
    HDHTTestOperator hds = new HDHTTestOperator();
    hds.setNumberOfBuckets(2);
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    FileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();
    ((MockFileAccess)bfs).disableChecksum();
    FileAccessFSImpl walFs = new MockFileAccess();
    walFs.setBasePath(file.getAbsolutePath() + "/WAL/");
    walFs.init();
    ((MockFileAccess)walFs).disableChecksum();

    hds.setFileStore(bfs);
    hds.setWalStore(walFs);
    hds.setFlushSize(1);
    hds.setFlushIntervalCount(1);
    hds.setup(new OperatorContextTestHelper.TestIdOperatorContext(1, new DefaultAttributeMap()));

    hds.writeExecutor = MoreExecutors.sameThreadExecutor();

    hds.beginWindow(1);
    hds.put(0, getLongByteArray(1), getLongByteArray(10).toByteArray());
    hds.put(1, getLongByteArray(1), getLongByteArray(100).toByteArray());
    hds.endWindow();
    hds.checkpointed(1);

    hds.beginWindow(2);
    hds.put(0, getLongByteArray(1), getLongByteArray(20).toByteArray());
    hds.put(1, getLongByteArray(1), getLongByteArray(200).toByteArray());
    hds.endWindow();
    hds.checkpointed(2);

    // Commit window id 2
    hds.committed(2);
    logger.debug("Bucket keys = " + hds.bucketKeys);
    hds.setPartitionCount(2);

    hds.beginWindow(3);
    hds.put(0, getLongByteArray(1), getLongByteArray(30).toByteArray());
    hds.put(1, getLongByteArray(1), getLongByteArray(300).toByteArray());
    hds.endWindow();
    hds.checkpointed(3);
    hds.forceWal();

    // use checkpoint after window 3 for recovery.
    HDHTTestOperator initialState = KryoCloneUtils.cloneObject(new Kryo(), hds);

    StatsListener.Response rsp = hds.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    List<Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>(hds));

    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>> newPartitions = initialState.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals(2, newPartitions.size());

    Map<Integer, Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>> m = Maps.newHashMap();
    for (Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>> p : newPartitions) {
      m.put(m.size(), p);
    }

    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    int opId = 2;
    List<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>> opers = Lists.newArrayList();
    for (Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>> p : newPartitions) {
      HDHTTestOperator oi = (HDHTTestOperator)p.getPartitionedInstance();

      oi.setFileStore(bfs);
      oi.setWalStore(walFs);
      oi.setFlushSize(1);
      oi.setFlushIntervalCount(1);
      oi.setup(new OperatorContextTestHelper.TestIdOperatorContext(opId++, new DefaultAttributeMap()));
      opers.add(oi);
    }

    logger.info("Partition 0: Bucket keys = {} Partiton keys = {} Partitions = {} ", opers.get(0).bucketKeys, opers.get(0).partitionMask, opers.get(0).partitions);
    logger.info("Partition 1: Bucket keys = {} Partiton keys = {} Partitions = {} ", opers.get(1).bucketKeys, opers.get(1).partitionMask, opers.get(1).partitions);

    // This should run recovery, as first tuple is added in bucket
    opers.get(0).beginWindow(4);
    opers.get(1).beginWindow(4);
    opers.get(0).put(0, getLongByteArray(2), getLongByteArray(800).toByteArray());
    opers.get(1).put(1, getLongByteArray(2), getLongByteArray(1000).toByteArray());

    opers.get(0).endWindow();
    opers.get(0).checkpointed(4);
    opers.get(1).endWindow();
    opers.get(1).checkpointed(4);

    /* The latest value is recovered from WAL */
    Assert.assertEquals("Value of key=1, bucket=0 is recovered from WAL", 30, getLong(opers.get(0).getUncommitted(0, getLongByteArray(1))));
    Assert.assertEquals("Value of key=1, bucket=1 is recovered from WAL", 300, getLong(opers.get(1).getUncommitted(1, getLongByteArray(1))));
    Assert.assertNull(opers.get(0).getUncommitted(1, getLongByteArray(1)));
    Assert.assertNull(opers.get(1).getUncommitted(0, getLongByteArray(1)));

    Assert.assertEquals("Value of key=1, bucket=0 is recovered from WAL", 20, getLong(opers.get(0).get(0, getLongByteArray(1))));
    Assert.assertEquals("Value of key=1, bucket=1 is recovered from WAL", 200, getLong(opers.get(1).get(1, getLongByteArray(1))));
    Assert.assertNull(opers.get(0).getUncommitted(1, getLongByteArray(1)));
    Assert.assertNull(opers.get(1).getUncommitted(0, getLongByteArray(1)));
  }

  /**
   * Test for testing dynamic partitioning. Check that recovery from previous
   * WAL is performed as expected when number of partitions is decreased from 2
   * to 1 Create a 2 partitions of operators, 1 bucket per partition Dynamically
   * change partition count to 1: Both the buckets are now managed by same
   * partition. Run recovery and check that committed and uncommitted WAL values
   * are returned appropriately after partitioning In this test, the new
   * partitions have 2 parent WALs. This test validates that weaving of WAL
   * files is done as expected per checkpointed window ID.
   */
  @Test
  public void testPartitioningMultipleParentWals() throws Exception
  {
    HDHTTestOperator hds = new HDHTTestOperator();
    hds.setNumberOfBuckets(2);
    hds.setPartitionCount(2);

    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    FileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();
    ((MockFileAccess)bfs).disableChecksum();
    FileAccessFSImpl walFs = new MockFileAccess();
    walFs.setBasePath(file.getAbsolutePath() + "/WAL/");
    walFs.init();
    ((MockFileAccess)walFs).disableChecksum();

    //Create 2 partitions
    HDHTTestOperator initialState = KryoCloneUtils.cloneObject(new Kryo(), hds);
    StatsListener.Response rsp = hds.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    List<Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>(hds));

    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>> newPartitions = initialState.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals(2, newPartitions.size());

    Map<Integer, Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>> m = Maps.newHashMap();
    for (Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>> p : newPartitions) {
      m.put(m.size(), p);
    }

    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    int opId = 2;
    List<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>> opers = Lists.newArrayList();
    for (Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>> p : newPartitions) {
      HDHTTestOperator oi = (HDHTTestOperator)p.getPartitionedInstance();

      oi.setFileStore(bfs);
      oi.setWalStore(walFs);
      oi.setFlushSize(1);
      oi.setFlushIntervalCount(1);
      oi.setup(new OperatorContextTestHelper.TestIdOperatorContext(opId++, new DefaultAttributeMap()));
      opers.add(oi);
    }
    opers.get(0).writeExecutor = MoreExecutors.sameThreadExecutor();
    opers.get(1).writeExecutor = MoreExecutors.sameThreadExecutor();

    logger.info("Partition 0: Bucket keys = {} Partiton keys = {} Partitions = {} ", opers.get(0).bucketKeys, opers.get(0).partitionMask, opers.get(0).partitions);
    logger.info("Partition 1: Bucket keys = {} Partiton keys = {} Partitions = {} ", opers.get(1).bucketKeys, opers.get(1).partitionMask, opers.get(1).partitions);

    opers.get(0).beginWindow(1);
    opers.get(0).put(0, getLongByteArray(1), getLongByteArray(10).toByteArray());
    opers.get(0).endWindow();
    opers.get(0).checkpointed(1);

    opers.get(1).beginWindow(1);
    opers.get(1).put(1, getLongByteArray(1), getLongByteArray(100).toByteArray());
    opers.get(1).endWindow();
    opers.get(1).checkpointed(1);

    opers.get(0).beginWindow(2);
    opers.get(0).put(0, getLongByteArray(1), getLongByteArray(20).toByteArray());
    opers.get(0).endWindow();
    opers.get(0).checkpointed(2);

    opers.get(1).beginWindow(2);
    opers.get(1).put(1, getLongByteArray(1), getLongByteArray(200).toByteArray());
    opers.get(1).endWindow();
    opers.get(1).checkpointed(2);

    // Commit window id 2
    opers.get(0).committed(2);
    opers.get(1).committed(2);

    opers.get(0).setPartitionCount(1);

    opers.get(0).beginWindow(3);
    opers.get(0).put(0, getLongByteArray(1), getLongByteArray(30).toByteArray());
    opers.get(0).endWindow();
    opers.get(0).checkpointed(3);
    opers.get(0).forceWal();

    opers.get(1).beginWindow(3);
    opers.get(1).put(1, getLongByteArray(1), getLongByteArray(300).toByteArray());
    opers.get(1).endWindow();
    opers.get(1).checkpointed(3);
    opers.get(1).forceWal();

    // use checkpoint after window 3 for recovery.
    initialState = (HDHTTestOperator)KryoCloneUtils.cloneObject(new Kryo(), opers.get(0));
    rsp = hds.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>(opers.get(0)));
    partitions.add(new DefaultPartition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>>(opers.get(1)));

    // incremental capacity controlled partitionCount property
    newPartitions = initialState.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals(1, newPartitions.size());
    m = Maps.newHashMap();
    for (Partition<AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>> p : newPartitions) {
      m.put(m.size(), p);
    }

    initialState.partitioned(m);
    Assert.assertEquals(1, initialState.getCurrentPartitions());

    HDHTTestOperator newOperator = (HDHTTestOperator)newPartitions.iterator().next().getPartitionedInstance();

    newOperator.setFileStore(bfs);
    newOperator.setWalStore(walFs);
    newOperator.setFlushSize(1);
    newOperator.setFlushIntervalCount(1);
    newOperator.setup(new OperatorContextTestHelper.TestIdOperatorContext(opId++, new DefaultAttributeMap()));

    // This should run recovery, as first tuple is added in bucket
    newOperator.beginWindow(4);
    ;
    newOperator.put(0, getLongByteArray(2), getLongByteArray(800).toByteArray());
    newOperator.put(1, getLongByteArray(2), getLongByteArray(1000).toByteArray());

    newOperator.endWindow();
    newOperator.checkpointed(4);

    /* The latest value is recovered from WAL */
    Assert.assertEquals("Value of key=1, bucket=0 is recovered from WAL", 30, getLong(newOperator.getUncommitted(0, getLongByteArray(1))));
    Assert.assertEquals("Value of key=1, bucket=1 is recovered from WAL", 300, getLong(newOperator.getUncommitted(1, getLongByteArray(1))));
    Assert.assertEquals("Value of key=1, bucket=0 is recovered from WAL", 800, getLong(newOperator.getUncommitted(0, getLongByteArray(2))));
    Assert.assertEquals("Value of key=1, bucket=1 is recovered from WAL", 1000, getLong(newOperator.getUncommitted(1, getLongByteArray(2))));

    Assert.assertEquals("Value of key=1, bucket=0 is recovered from WAL", 20, getLong(newOperator.get(0, getLongByteArray(1))));
    Assert.assertEquals("Value of key=1, bucket=1 is recovered from WAL", 200, getLong(newOperator.get(1, getLongByteArray(1))));
  }
}
