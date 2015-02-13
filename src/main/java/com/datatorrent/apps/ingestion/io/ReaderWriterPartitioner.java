/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.ingestion.io;

import java.io.ByteArrayOutputStream;
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

public class ReaderWriterPartitioner implements Partitioner<BlockReader>, StatsListener
{
  //should be power of 2
  protected transient int maxPartition;

  //should be power of 2
  protected transient int minPartition;
  /**
   * Interval at which stats are processed. Default : 1 minute
   */
  private long intervalMillis;

  private transient final StatsListener.Response response;

  private transient int partitionCount;

  private transient long nextMillis;

  private transient final Map<Integer, Long> readerBacklog;

  protected transient final Map<Integer, BasicCounters<MutableLong>> readerCounters;

  protected transient Map<Integer, BasicCounters<MutableLong>> writerCounters;

  private transient long maxReaderThroughput;

  private transient int threshold;

  private final long spinMillis;

  private final long readerAppWindow;

  private final long writerAppWindow;

  private boolean isWindowCompletelyUtilized;

  public ReaderWriterPartitioner(long readerAppWindow, long writerAppWindow, long spinMillis)
  {
    response = new StatsListener.Response();

    readerBacklog = Maps.newHashMap();
    readerCounters = Maps.newHashMap();
    writerCounters = Maps.newHashMap();

    partitionCount = 1;
    maxPartition = 16;
    minPartition = 1;
    intervalMillis = 10000L; //10 seconds

    threshold = 1;

    this.spinMillis = spinMillis;
    this.readerAppWindow = readerAppWindow;
    this.writerAppWindow = writerAppWindow;
  }

  @Override
  public Collection<Partition<BlockReader>> definePartitions(Collection<Partition<BlockReader>> collection,
                                                             PartitioningContext partitioningContext)
  {
    //sync the stats listener properties with the operator
    Partition<BlockReader> readerPartition = collection.iterator().next();

    if (maxReaderThroughput != readerPartition.getPartitionedInstance().maxThroughput) {
      LOG.debug("maxReaderThroughput: from {} to {}", maxReaderThroughput, readerPartition.getPartitionedInstance().maxThroughput);
      maxReaderThroughput = readerPartition.getPartitionedInstance().maxThroughput;
    }

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

    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<BlockReader>> partitionIterator = collection.iterator();
      while (morePartitionsToCreate++ < 0) {
        Partition<BlockReader> toRemove = partitionIterator.next();
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
        kryo.writeObject(loutput, this);
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
    return collection;
  }

  @Override
  public void partitioned(Map<Integer, Partition<BlockReader>> map)
  {
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    response.repartitionRequired = false;

    List<Stats.OperatorStats> lastWindowedStats = stats.getLastWindowedStats();

    if (lastWindowedStats != null && lastWindowedStats.size() > 0) {
      long backlog = 0;
      boolean isReader = false;
      for (int i = lastWindowedStats.size() - 1; i >= 0; i--) {
        Object counters = lastWindowedStats.get(i).counters;

        if (counters != null) {
          if (counters instanceof BlockReader.BlockReaderCounters) {
            isReader = true;
            BlockReader.BlockReaderCounters lcounters = (BlockReader.BlockReaderCounters) counters;
            backlog = lastWindowedStats.get(i).inputPorts.get(0).queueSize;
            backlog += lcounters.counters.getCounter(AbstractBlockReader.ReaderCounterKeys.BACKLOG).longValue();

            readerCounters.put(stats.getOperatorId(), lcounters.counters);
          }
          else if (counters instanceof BlockWriter.BlockWriterCounters) {
            writerCounters.put(stats.getOperatorId(), ((BlockWriter.BlockWriterCounters) counters).counters);
          }
          break;
        }
      }
      if (isReader) {
        readerBacklog.put(stats.getOperatorId(), backlog);
      }
    }

    if (System.currentTimeMillis() < nextMillis) {
      return response;
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
          response.repartitionRequired = true;
          clearState();
          return response;
        }
      }
      else {
        //outstanding work. we cannot go beyond maxThroughput and max partitions.
        long totalBlocksRead = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.BLOCKS);
        long totalBlocksWritten = getTotalOf(writerCounters, BlockWriter.BlockKeys.BLOCKS);

        long totalReadTime = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.TIME);
        long totalWriteTime = getTotalOf(writerCounters, BlockWriter.Counters.TOTAL_TIME_ELAPSED);

        long readerIdleTime = readerAppWindow * spinMillis - totalReadTime;
        long writerIdleTime = writerAppWindow * spinMillis - totalWriteTime;

        if (!isWindowCompletelyUtilized && readerIdleTime > 0 && writerIdleTime > 0) {
          //need to increase the threshold for block readers.

          long readTimePerBlock = totalBlocksRead / totalReadTime;
          long writeTimePerBlock = totalBlocksWritten / totalWriteTime;

          long timePerBlock = Math.max(readTimePerBlock, writeTimePerBlock);
          long idleTime = Math.min(readerIdleTime, writerIdleTime);

          int moreBlocks = (int) (idleTime / timePerBlock);
          if (moreBlocks > 0) {
            threshold += moreBlocks;

            LOG.debug("threshold change to {}", threshold);
            clearState();
            response.repartitionRequired = true;
            //In order to decide how to partition we need the correct outstanding number. if the operator was
            //idling then we first increase the threshold which will decrease the backlog.
            //Only when the operator doesn't idle in a window we create multiple partitions to increase throughput.
            isWindowCompletelyUtilized = true;
            return response;
          }
        }
        if (totalBacklog == partitionCount) {
          clearState();
          return response; //do not repartition
        }
        int newPartitionCount;

        if (totalBacklog < partitionCount) {
          newPartitionCount = (int) totalBacklog;
        }
        else {
          long totalReadBytes = getTotalOf(readerCounters, BlockReader.ReaderCounterKeys.BYTES);
          long bytesReadPerSec = totalReadBytes / totalReadTime;

          int newCountByThroughput = (int) ((maxReaderThroughput - bytesReadPerSec) / (bytesReadPerSec / partitionCount));

          if (maxReaderThroughput > 0 && newCountByThroughput <= maxPartition) {
            LOG.debug("countByThroughput {}", newCountByThroughput);
            if (newCountByThroughput < 0) {
              //we can't scale up
              newPartitionCount = partitionCount;
            }
            else {
              if (totalBacklog > newCountByThroughput) {
                newPartitionCount = newCountByThroughput;
              }
              else {
                newPartitionCount = (int) totalBacklog;
              }
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
        clearState();

        if (newPartitionCount == partitionCount) {
          return response; //do not repartition
        }

        //partition count can only be a power of 2. so adjusting newPartitionCount if it isn't
        newPartitionCount = getAdjustedCount(newPartitionCount);

        partitionCount = newPartitionCount;
        response.repartitionRequired = true;
        LOG.debug("end listener", totalBacklog, partitionCount);

        return response;

      }
    }
    return response;
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

  int getPartitionCount()
  {
    return partitionCount;
  }

  int getThreshold()
  {
    return threshold;
  }

  public void setIntervalMillis(long millis)
  {
    this.intervalMillis = millis;
  }

  public long getIntervalMillis()
  {
    return intervalMillis;
  }

  private static final Logger LOG = LoggerFactory.getLogger(ReaderWriterPartitioner.class);

}
