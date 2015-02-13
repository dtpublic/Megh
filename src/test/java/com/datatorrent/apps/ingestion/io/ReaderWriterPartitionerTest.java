package com.datatorrent.apps.ingestion.io;

import java.util.List;

import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Lists;

import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;

import com.datatorrent.lib.counters.BasicCounters;

public class ReaderWriterPartitionerTest
{
  static class TestMeta extends TestWatcher
  {
    ReaderWriterPartitioner partitioner;

    @Override
    protected void starting(Description description)
    {
      partitioner = new ReaderWriterPartitioner(4, 4, 500);
      partitioner.setIntervalMillis(500);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

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
  public void testProcessStatsForThreshold()
  {
    PseudoBatchedOperatorStats writerStats = new PseudoBatchedOperatorStats(1);
    writerStats.operatorStats = Lists.newArrayList();
    writerStats.operatorStats.add(new WriterStats(1, 100, 1));

    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 1, 100, 1));

    StatsListener.Response response = testMeta.partitioner.processStats(writerStats);
    Assert.assertFalse("no partitioning", response.repartitionRequired);

    response = testMeta.partitioner.processStats(readerStats);
    Assert.assertTrue("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count same", 1, testMeta.partitioner.getPartitionCount());
    Assert.assertEquals("threshold changed", 2000, testMeta.partitioner.getThreshold());
  }

  @Test
  public void testProcessStatsForPartitionCount() throws InterruptedException
  {
    PseudoBatchedOperatorStats writerStats = new PseudoBatchedOperatorStats(1);
    writerStats.operatorStats = Lists.newArrayList();
    writerStats.operatorStats.add(new WriterStats(1, 100, 1));

    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 1, 100, 1));

    testMeta.partitioner.processStats(writerStats);
    testMeta.partitioner.processStats(readerStats);

    Thread.sleep(500);

    testMeta.partitioner.processStats(writerStats);
    StatsListener.Response response = testMeta.partitioner.processStats(readerStats);
    Assert.assertTrue("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count changed", 8, testMeta.partitioner.getPartitionCount());
    Assert.assertEquals("threshold same", 2000, testMeta.partitioner.getThreshold());
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

  static class ReaderStats extends Stats.OperatorStats
  {

    ReaderStats(long backlog, long readBlocks, long bytes, long time)
    {
      BasicCounters<MutableLong> bc = new BasicCounters<MutableLong>(MutableLong.class);
      bc.setCounter(BlockReader.ReaderCounterKeys.BACKLOG, new MutableLong(backlog));
      bc.setCounter(BlockReader.ReaderCounterKeys.BLOCKS, new MutableLong(readBlocks));
      bc.setCounter(BlockReader.ReaderCounterKeys.BYTES, new MutableLong(bytes));
      bc.setCounter(BlockReader.ReaderCounterKeys.TIME, new MutableLong(time));

      counters = new BlockReader.BlockReaderCounters(bc);

      PortStats portStats = new PortStats("blocks");
      portStats.queueSize = 0;
      inputPorts = Lists.newArrayList(portStats);
    }
  }

  static class WriterStats extends Stats.OperatorStats
  {
    WriterStats(long writtenBlocks, long bytes, long time)
    {
      BasicCounters<MutableLong> bc = new BasicCounters<MutableLong>(MutableLong.class);
      bc.setCounter(BlockWriter.BlockKeys.BLOCKS, new MutableLong(writtenBlocks));
      bc.setCounter(BlockWriter.Counters.TOTAL_BYTES_WRITTEN, new MutableLong(bytes));
      bc.setCounter(BlockWriter.Counters.TOTAL_TIME_ELAPSED, new MutableLong(time));

      counters = new BlockWriter.BlockWriterCounters(bc);
    }
  }
}