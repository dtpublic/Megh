/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion.io;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.*;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.*;

import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;

public class ReaderWriterPartitioner implements Partitioner<BlockReader>, StatsListener, Serializable
{
  //should be power of 2
  private int maxPartition;

  //should be power of 2
  private int minPartition;
  /**
   * Interval at which stats are processed. Default : 10 seconds
   */
  private long intervalMillis;

  private int partitionCount;

  private final StatsListener.Response readerResponse;

  private final StatsListener.Response writerResponse;

  private final Map<Integer, Long> readerBacklog;

  protected final Map<Integer, BasicCounters<MutableLong>> readerCounters;

  protected final Map<Integer, BasicCounters<MutableLong>> writerCounters;

  private final long streamingWindowSize;

  private final int readerAppWindow;

  private final int writerAppWindow;

  private int threshold;

  private transient long maxReaderThroughput;

  private transient long nextMillis;

  private transient boolean isWindowCompletelyUtilized;

  public ReaderWriterPartitioner(int readerAppWindow, int writerAppWindow, long streamingWindowSize)
  {
    readerResponse = new StatsListener.Response();
    writerResponse = new StatsListener.Response();

    readerBacklog = Maps.newHashMap();
    readerCounters = Maps.newHashMap();
    writerCounters = Maps.newHashMap();

    partitionCount = 1;
    maxPartition = 16;
    minPartition = 1;
    intervalMillis = 10000L; //10 seconds

    threshold = 1;
    this.streamingWindowSize = streamingWindowSize;
    this.readerAppWindow = readerAppWindow;
    this.writerAppWindow = writerAppWindow;
  }

  @Override
  public Collection<Partition<BlockReader>> definePartitions(Collection<Partition<BlockReader>> collection,
                                                             PartitioningContext partitioningContext)
  {
    //sync the stats listener properties with the operator
    Partition<BlockReader> readerPartition = collection.iterator().next();

    //changes max throughput on the partitioner
    if (maxReaderThroughput != readerPartition.getPartitionedInstance().maxThroughput) {
      LOG.debug("maxReaderThroughput: from {} to {}", maxReaderThroughput, readerPartition.getPartitionedInstance().maxThroughput);
      maxReaderThroughput = readerPartition.getPartitionedInstance().maxThroughput;
    }

    //this changes threshold on the operators
    if (threshold != readerPartition.getPartitionedInstance().getThreshold()) {
      LOG.debug("threshold: from {} to {}", readerPartition.getPartitionedInstance().getThreshold(), threshold);
      for (Partition<BlockReader> p : collection) {
        p.getPartitionedInstance().setThreshold(threshold);
      }
    }

    if (readerPartition.getStats() == null) {
      //First time when define partitions is called, no partitioning required
      return collection;
    }

    //Collect state here
    List<BlockMetadata.FileBlockMetadata> pendingBlocks = Lists.newArrayList();
    for (Partition<BlockReader> partition : collection) {
      pendingBlocks.addAll(partition.getPartitionedInstance().getBlocksQueue());
    }

    int morePartitionsToCreate = partitionCount - collection.size();
    List<BasicCounters<MutableLong>> deletedCounters = Lists.newArrayList();

    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<BlockReader>> partitionIterator = collection.iterator();
      while (morePartitionsToCreate++ < 0) {
        Partition<BlockReader> toRemove = partitionIterator.next();
        deletedCounters.add(toRemove.getPartitionedInstance().getCounters());

        LOG.debug("partition removed {}", toRemove.getPartitionedInstance().getOperatorId());
        partitionIterator.remove();
      }
    }
    else {
      //Add more partitions
      Kryo kryo = new Kryo();
      while (morePartitionsToCreate-- > 0) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output loutput = new Output(bos);
        kryo.writeObject(loutput, readerPartition.getPartitionedInstance());
        loutput.close();
        Input lInput = new Input(bos.toByteArray());

        @SuppressWarnings("unchecked")
        BlockReader blockReader = kryo.readObject(lInput, BlockReader.class);

        DefaultPartition<BlockReader> partition = new DefaultPartition<BlockReader>(blockReader);
        collection.add(partition);
      }
    }
    @SuppressWarnings("unchecked")
    DefaultInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = (DefaultInputPort<BlockMetadata
      .FileBlockMetadata>) partitioningContext.getInputPorts().get(0);

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(collection), blocksMetadataInput);
    int lPartitionMask = collection.iterator().next().getPartitionKeys().get(blocksMetadataInput).mask;

    //transfer the state here
    for (Partition<BlockReader> newPartition : collection) {
      BlockReader reader = newPartition.getPartitionedInstance();

      reader.setPartitionKeys(newPartition.getPartitionKeys().get(blocksMetadataInput).partitions);
      reader.setPartitionMask(lPartitionMask);
      LOG.debug("partitions {},{}", reader.getPartitionKeys(), reader.getPartitionMask());
      reader.clearBlockQueue();

      //distribute block-metadatas
      Iterator<BlockMetadata.FileBlockMetadata> pendingBlocksIterator = pendingBlocks.iterator();
      while (pendingBlocksIterator.hasNext()) {
        BlockMetadata.FileBlockMetadata pending = pendingBlocksIterator.next();
        if (reader.getPartitionKeys().contains(pending.hashCode() & lPartitionMask)) {
          reader.addBlockMetadata(pending);
          pendingBlocksIterator.remove();
        }
      }
    }

    //transfer the counters
    BlockReader targetReader = collection.iterator().next().getPartitionedInstance();
    for (BasicCounters<MutableLong> removedCounter : deletedCounters) {
      addCounters(targetReader.getCounters(), removedCounter);
    }

    return collection;
  }

  protected void addCounters(BasicCounters<MutableLong> target, BasicCounters<MutableLong> source)
  {

    for (Enum<AbstractBlockReader.ReaderCounterKeys> key : AbstractBlockReader.ReaderCounterKeys.values()) {
      MutableLong tcounter = target.getCounter(key);
      if (tcounter == null) {
        tcounter = new MutableLong();
        target.setCounter(key, tcounter);
      }
      MutableLong scounter = source.getCounter(key);
      if (scounter != null) {
        tcounter.add(scounter.longValue());
      }
    }
    for (Enum<BlockReader.BlockKeys> key : BlockReader.BlockKeys.values()) {
      MutableLong tcounter = target.getCounter(key);
      if (tcounter == null) {
        tcounter = new MutableLong();
        target.setCounter(key, tcounter);
      }
      MutableLong scounter = source.getCounter(key);
      if (scounter != null) {
        tcounter.add(scounter.longValue());
      }
    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<BlockReader>> map)
  {
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    readerResponse.repartitionRequired = false;

    List<Stats.OperatorStats> lastWindowedStats = stats.getLastWindowedStats();
    boolean isReader = false;

    if (lastWindowedStats != null && lastWindowedStats.size() > 0) {
      long backlog = 0;
      for (int i = lastWindowedStats.size() - 1; i >= 0; i--) {
        Object counters = lastWindowedStats.get(i).counters;

        if (counters != null) {
          @SuppressWarnings("unchecked")
          BasicCounters<MutableLong> lcounters = (BasicCounters<MutableLong>) counters;
          if (lcounters.getCounter(BlockReader.ReaderCounterKeys.BLOCKS) != null) {
            isReader = true;
            int queueSize = lastWindowedStats.get(i).inputPorts.get(0).queueSize;
            if (queueSize > 1) {
              backlog = queueSize;
            }
            backlog += lcounters.getCounter(AbstractBlockReader.ReaderCounterKeys.BACKLOG).longValue();

            readerCounters.put(stats.getOperatorId(), lcounters);
          }
          else if (lcounters.getCounter(BlockWriter.BlockKeys.BLOCKS) != null) {
            writerCounters.put(stats.getOperatorId(), lcounters);
          }
          break;
        }
      }
      if (isReader) {
        readerBacklog.put(stats.getOperatorId(), backlog);
      }
    }

    if (System.currentTimeMillis() < nextMillis || !isReader) {
      return isReader ? readerResponse : writerResponse;
    }

    //check if partitioning is needed after stats from all the operators are received.
    if (readerCounters.size() == partitionCount && writerCounters.size() == partitionCount) {

      nextMillis = System.currentTimeMillis() + intervalMillis;
      LOG.debug("Proposed NextMillis = {}", nextMillis);

      long totalBacklog = 0;
      for (Map.Entry<Integer, Long> backlog : readerBacklog.entrySet()) {
        totalBacklog += backlog.getValue();
      }

      LOG.debug("total backlog {} partitions {} threshold {}", totalBacklog, partitionCount, threshold);

      if (totalBacklog <= 0) {
        //scale down completely
        LOG.debug("no backlog");
        if (partitionCount > minPartition) {
          LOG.debug("partition change to {}", minPartition);
          partitionCount = minPartition;
          readerResponse.repartitionRequired = true;
          clearState();
          return readerResponse;
        }
      }
      else {
        //outstanding work. we cannot go beyond maxThroughput and max partitions.
        long totalBlocksRead = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.BLOCKS);
        long totalBlocksWritten = getTotalOf(writerCounters, BlockWriter.BlockKeys.BLOCKS);

        long totalReadTime = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.TIME);
        long totalWriteTime = getTotalOf(writerCounters, BlockWriter.Counters.TOTAL_TIME_WRITING_MILLISECONDS);

        long maxReadTimeWindow = getMaxOf(readerCounters, BlockReader.BlockKeys.READ_TIME_WINDOW);
        long maxWriteTimeWindow = getMaxOf(writerCounters, BlockWriter.BlockKeys.WRITE_TIME_WINDOW);

        long readerIdleTime = readerAppWindow * streamingWindowSize - maxReadTimeWindow;
        long writerIdleTime = writerAppWindow * streamingWindowSize - maxWriteTimeWindow;

        LOG.debug("total block: reader {} and writer {}", totalBlocksRead, totalBlocksWritten);
        LOG.debug("total time: reader {} and writer {}", totalReadTime, totalWriteTime);
        LOG.debug("max time: reader {} and writer {}", maxReadTimeWindow, maxWriteTimeWindow);
        LOG.debug("idle time: reader {} and writer {}", readerIdleTime, writerIdleTime);

        if (!isWindowCompletelyUtilized && readerIdleTime > 0 && writerIdleTime > 0) {
          //need to increase the threshold for block readers.

          double readTimePerBlock = totalReadTime / (totalBlocksRead * 1.0);
          double writeTimePerBlock = totalWriteTime / (totalBlocksWritten * 1.0);

          double timePerBlock = Math.max(readTimePerBlock, writeTimePerBlock);
          long idleTime = Math.min(readerIdleTime, writerIdleTime);

          int moreBlocks = 0;
          if (timePerBlock != 0) {
            moreBlocks = (int) (idleTime / timePerBlock);
          }
          if (moreBlocks > 0) {
            threshold += moreBlocks;

            LOG.debug("threshold change to {}", threshold);
            clearState();
            readerResponse.repartitionRequired = true;
            //In order to decide how to partition we need the correct outstanding number. if the operator was
            //idling then we first increase the threshold which will decrease the backlog.
            //Only when the operator doesn't idle in a window we create multiple partitions to increase throughput.
            isWindowCompletelyUtilized = true;
            return readerResponse;
          }
        }
        if (isWindowCompletelyUtilized && (readerIdleTime < 0 || writerIdleTime < 0)) {
          //we are doing more work in a window. need to reduce.
          if (threshold > 1) {
            threshold--;
            LOG.debug("threshold reduced to {}", threshold);
            clearState();
            readerResponse.repartitionRequired = true;
            return readerResponse;
          }
        }

        if (totalBacklog == partitionCount) {
          clearState();
          return readerResponse; //do not repartition
        }

        int newPartitionCount;

        if (totalBacklog < partitionCount) {
          newPartitionCount = (int) totalBacklog;
        }
        else {
          long totalReadBytes = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.BYTES);
          long bytesReadPerSec = totalReadBytes / totalReadTime;

          int newCountByThroughput = partitionCount + (int) ((maxReaderThroughput - bytesReadPerSec) / (bytesReadPerSec / partitionCount));
          LOG.debug("countByThroughput {}", newCountByThroughput);

          if (maxReaderThroughput > 0 && newCountByThroughput < partitionCount) {
            //can't scale up since throughput limit is reached.
            newPartitionCount = partitionCount;
          }
          else if (maxReaderThroughput > 0 && newCountByThroughput <= maxPartition) {
            if (totalBacklog > newCountByThroughput) {
              newPartitionCount = newCountByThroughput;
            }
            else {
              newPartitionCount = (int) totalBacklog;
            }
          }
          else {
            LOG.debug("byMaxPartition {}", maxPartition);
            if (totalBacklog > maxPartition) {
              newPartitionCount = maxPartition;
            }
            else {
              newPartitionCount = (int) totalBacklog;
            }
          }
        }

        if (newPartitionCount == partitionCount) {
          return readerResponse; //do not repartition
        }

        //partition count can only be a power of 2. so adjusting newPartitionCount if it isn't
        newPartitionCount = getAdjustedCount(newPartitionCount);

        partitionCount = newPartitionCount;
        readerResponse.repartitionRequired = true;
        clearState();
        LOG.debug("end listener {} {}", totalBacklog, partitionCount);
        return readerResponse;
      }
    }
    return readerResponse;
  }

  private void clearState()
  {
    readerBacklog.clear();
    readerCounters.clear();
    writerCounters.clear();
  }

  protected int getAdjustedCount(int newCount)
  {
    Preconditions.checkArgument(newCount <= maxPartition && newCount >= minPartition, newCount);

    int adjustCount = 1;
    while (adjustCount < newCount) {
      adjustCount <<= 1;
    }
    if (adjustCount > newCount) {
      adjustCount >>>= 1;
    }
    LOG.debug("adjust {} => {}", newCount, adjustCount);
    return adjustCount;
  }

  private long getTotalOf(Map<Integer, BasicCounters<MutableLong>> countersMap, Enum<?> key)
  {
    long total = 0;
    for (BasicCounters<MutableLong> counters : countersMap.values()) {
      total += counters.getCounter(key).longValue();
    }
    return total;
  }

  @SuppressWarnings("unused")
  private long getMinOf(Map<Integer, BasicCounters<MutableLong>> countersMap, Enum<?> key)
  {
    long min = -1;
    for (BasicCounters<MutableLong> counters : countersMap.values()) {
      long val = counters.getCounter(key).longValue();
      if (min == -1 || val < min) {
        min = val;
      }
    }
    return min;
  }

  @SuppressWarnings("unused")
  private long getMaxOf(Map<Integer, BasicCounters<MutableLong>> countersMap, Enum<?> key)
  {
    long max = -1;
    for (BasicCounters<MutableLong> counters : countersMap.values()) {
      long val = counters.getCounter(key).longValue();
      if (val > max) {
        max = val;
      }
    }
    return max;
  }

  Response getResponse()
  {
    return readerResponse;
  }

  int getMaxPartition()
  {
    return maxPartition;
  }

  int getMinPartition()
  {
    return minPartition;
  }

  int getPartitionCount()
  {
    return partitionCount;
  }

  void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  int getThreshold()
  {
    return threshold;
  }

  long getMaxReaderThroughput()
  {
    return maxReaderThroughput;
  }

  void setMaxReaderThroughput(long throughput)
  {
    this.maxReaderThroughput = throughput;
  }

  public void setIntervalMillis(long millis)
  {
    this.intervalMillis = millis;
  }

  public long getIntervalMillis()
  {
    return intervalMillis;
  }

  private static final long serialVersionUID = 201502130023L;

  private static final Logger LOG = LoggerFactory.getLogger(ReaderWriterPartitioner.class);

}
