/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion.io;

import java.io.*;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Lists;

import com.datatorrent.api.*;

import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.partitioner.StatelessPartitionerTest;

public class ReaderWriterPartitionerTest
{
  static class TestMeta extends TestWatcher
  {
    ReaderWriterPartitioner partitioner;

    @Override
    protected void starting(Description description)
    {
      partitioner = new ReaderWriterPartitioner();
      partitioner.setIntervalMillis(500);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testDeserialization() throws IOException, ClassNotFoundException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);

    oos.writeObject(testMeta.partitioner);
    oos.flush();
    baos.flush();
    oos.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);

    ReaderWriterPartitioner dePartitioner = (ReaderWriterPartitioner) ois.readObject();
    Assert.assertNotNull("response", dePartitioner.getResponse());
    Assert.assertEquals("partition count", 1, dePartitioner.getPartitionCount());
    Assert.assertEquals("max partition", 16, dePartitioner.getMaxPartition());
    Assert.assertEquals("min partition", 1, dePartitioner.getMinPartition());
  }

  @Test
  public void testAdjustedCount()
  {
    Assert.assertEquals("min", 1, testMeta.partitioner.getAdjustedCount(1));
    Assert.assertEquals("max", 16, testMeta.partitioner.getAdjustedCount(16));
    Assert.assertEquals("max-1", 8, testMeta.partitioner.getAdjustedCount(15));
    Assert.assertEquals("min+1", 2, testMeta.partitioner.getAdjustedCount(2));
    Assert.assertEquals("between 1", 4, testMeta.partitioner.getAdjustedCount(4));
    Assert.assertEquals("between 2", 4, testMeta.partitioner.getAdjustedCount(7));
    Assert.assertEquals("between 2", 8, testMeta.partitioner.getAdjustedCount(12));
    boolean caught = false;
    try {
      testMeta.partitioner.getAdjustedCount(20);
    }
    catch (IllegalArgumentException ex) {
      caught = true;
    }
    Assert.assertTrue(" > max", caught);

    caught = false;
    try {
      testMeta.partitioner.getAdjustedCount(0);
    }
    catch (IllegalArgumentException ex) {
      caught = true;
    }
    Assert.assertTrue(" < min", caught);
  }

  @Test
  public void testProcessStatsPartitionCount() throws InterruptedException
  {
    PseudoBatchedOperatorStats writerStats = new PseudoBatchedOperatorStats(1);
    writerStats.operatorStats = Lists.newArrayList();
    writerStats.operatorStats.add(new WriterStats(1));

    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 100, 1));

    testMeta.partitioner.processStats(writerStats);
    StatsListener.Response response = testMeta.partitioner.processStats(readerStats);
    Assert.assertTrue("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count changed", 8, testMeta.partitioner.getPartitionCount());
  }

  @Test
  public void testProcessStatsBandwidthControl() throws InterruptedException
  {
    testMeta.partitioner.setMaxReaderThroughput(200);
    PseudoBatchedOperatorStats writerStats = new PseudoBatchedOperatorStats(1);
    writerStats.operatorStats = Lists.newArrayList();
    writerStats.operatorStats.add(new WriterStats(1));

    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 100, 1));

    testMeta.partitioner.processStats(writerStats);
    StatsListener.Response response = testMeta.partitioner.processStats(readerStats);
    Assert.assertTrue("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count changed", 2, testMeta.partitioner.getPartitionCount());
  }

  @Test
  public void testProcessStatsBandwidthControlNoPartition() throws InterruptedException
  {
    testMeta.partitioner.setMaxReaderThroughput(100);
    PseudoBatchedOperatorStats writerStats = new PseudoBatchedOperatorStats(1);
    writerStats.operatorStats = Lists.newArrayList();
    writerStats.operatorStats.add(new WriterStats(1));

    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 100, 1));

    testMeta.partitioner.processStats(writerStats);
    testMeta.partitioner.processStats(readerStats);

    Thread.sleep(500);

    testMeta.partitioner.processStats(writerStats);
    StatsListener.Response response = testMeta.partitioner.processStats(readerStats);
    Assert.assertFalse("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count same", 1, testMeta.partitioner.getPartitionCount());
  }

  @Test
  public void testDefinePartitions() throws InterruptedException
  {
    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 100, 1));

    testMeta.partitioner.setPartitionCount(8);

    final BlockReader reader = new BlockReader();

    List<Partitioner.Partition<BlockReader>> partitions = Lists.newArrayList();

    DefaultPartition<BlockReader> apartition = new DefaultPartition<BlockReader>(reader);

    PseudoParttion pseudoParttion = new PseudoParttion(apartition, readerStats);
    partitions.add(pseudoParttion);

    List<Operator.InputPort<?>> ports = Lists.newArrayList();
    ports.add(reader.blocksMetadataInput);

    Collection<Partitioner.Partition<BlockReader>> newPartitions = testMeta.partitioner.definePartitions(partitions,
      new StatelessPartitionerTest.PartitioningContextImpl(ports, 0));
    Assert.assertEquals(8, newPartitions.size());
  }

  @Test
  public void testPropertySyncAfterDefinePartitions() throws InterruptedException
  {

    final BlockReader reader = new BlockReader();
    reader.setMaxThroughput(100);

    List<Partitioner.Partition<BlockReader>> partitions = Lists.newArrayList();

    DefaultPartition<BlockReader> apartition = new DefaultPartition<BlockReader>(reader);
    PseudoParttion pseudoParttion = new PseudoParttion(apartition, null);
    partitions.add(pseudoParttion);

    List<Operator.InputPort<?>> ports = Lists.newArrayList();
    ports.add(reader.blocksMetadataInput);

    testMeta.partitioner.definePartitions(partitions, new StatelessPartitionerTest.PartitioningContextImpl(ports, 0));

    Assert.assertEquals("max throughput", 100, testMeta.partitioner.getMaxReaderThroughput());
  }

  static class PseudoBatchedOperatorStats implements StatsListener.BatchedOperatorStats
  {

    final int operatorId;
    List<Stats.OperatorStats> operatorStats;

    PseudoBatchedOperatorStats(int operatorId)
    {
      this.operatorId = operatorId;
    }

    @Override
    public List<Stats.OperatorStats> getLastWindowedStats()
    {
      return operatorStats;
    }

    @Override
    public int getOperatorId()
    {
      return 0;
    }

    @Override
    public long getCurrentWindowId()
    {
      return 0;
    }

    @Override
    public long getTuplesProcessedPSMA()
    {
      return 0;
    }

    @Override
    public long getTuplesEmittedPSMA()
    {
      return 0;
    }

    @Override
    public double getCpuPercentageMA()
    {
      return 0;
    }

    @Override
    public long getLatencyMA()
    {
      return 0;
    }
  }

  static class PseudoParttion extends DefaultPartition<BlockReader>
  {

    PseudoParttion(DefaultPartition<BlockReader> defaultPartition, StatsListener.BatchedOperatorStats stats)
    {
      super(defaultPartition.getPartitionedInstance(), defaultPartition.getPartitionKeys(),
        defaultPartition.getLoad(), stats);

    }
  }

  static class ReaderStats extends Stats.OperatorStats
  {

    ReaderStats(int backlog, long bytes, long time)
    {
      BasicCounters<MutableLong> bc = new BasicCounters<MutableLong>(MutableLong.class);
      bc.setCounter(BlockReader.ReaderCounterKeys.BYTES, new MutableLong(bytes));
      bc.setCounter(BlockReader.ReaderCounterKeys.TIME, new MutableLong(time));
      counters = bc;
      PortStats portStats = new PortStats("readerPort");
      portStats.queueSize = backlog;
      inputPorts = Lists.newArrayList(portStats);
    }
  }

  static class WriterStats extends Stats.OperatorStats
  {
    WriterStats(int backlog)
    {
      BasicCounters<MutableLong> bc = new BasicCounters<MutableLong>(MutableLong.class);
      bc.setCounter(BlockWriter.Counters.TOTAL_BYTES_WRITTEN, new MutableLong());
      counters = bc;

      PortStats portStats = new PortStats("writerPort");
      portStats.queueSize = backlog;
      inputPorts = Lists.newArrayList(portStats);
    }
  }
}