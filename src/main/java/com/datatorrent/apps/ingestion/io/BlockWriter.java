/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.malhar.lib.io.block.AbstractBlockReader;
import com.datatorrent.malhar.lib.io.block.BlockMetadata;
import com.datatorrent.malhar.lib.io.fs.AbstractFileOutputOperator;
import com.google.common.collect.Lists;

/**
 * Writes a block to the fs.
 *
 * @author Yogi/Sandeep
 */
public class BlockWriter extends AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>> implements
  Partitioner<BlockWriter>
{
  public static final String SUBDIR_BLOCKS = "blocks";
  private transient List<BlockMetadata.FileBlockMetadata> blockMetadatas;

  public final transient DefaultInputPort<BlockMetadata.FileBlockMetadata> blockMetadataInput = new DefaultInputPort<BlockMetadata.FileBlockMetadata>()
  {
    @Override
    public void process(BlockMetadata.FileBlockMetadata blockMetadata)
    {
      blockMetadatas.add(blockMetadata);
      LOG.debug("received blockId {} for file {} ", blockMetadata.getBlockId(), blockMetadata.getFilePath());
    }
  };

  public final transient DefaultOutputPort<BlockMetadata.FileBlockMetadata> blockMetadataOutput = new DefaultOutputPort<BlockMetadata.FileBlockMetadata>();

  public BlockWriter()
  {
    super();
    blockMetadatas = Lists.newArrayList();
    //The base class puts a restriction that the file-path cannot be null. With this block writer it is
    //being initialized in setup and not through configuration. So setting it to empty string.
    filePath = "";
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    filePath = context.getValue(Context.DAGContext.APPLICATION_PATH) + Path.SEPARATOR + SUBDIR_BLOCKS;
    super.setup(context);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    streamsCache.asMap().clear();
    endOffsets.clear();

    for (BlockMetadata.FileBlockMetadata blockMetadata : blockMetadatas) {
      blockMetadataOutput.emit(blockMetadata);
    }
    blockMetadatas.clear();
  }

  @Override
  protected String getFileName(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    return Long.toString(tuple.getBlockId());
  }

  @Override
  protected byte[] getBytesForTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    return tuple.getRecord().buffer;
  }

  private static final Logger LOG = LoggerFactory.getLogger(BlockWriter.class);

  @Override
  public Collection<Partition<BlockWriter>> definePartitions(Collection<Partition<BlockWriter>> partitions, PartitioningContext context)
  {
    if (context.getParallelPartitionCount() == 0) {
      return partitions;
    }
    List<Partition<BlockWriter>> newPartitions = Lists.newArrayList();
    //Create new partitions
    for (Partition<BlockWriter> partition : partitions) {
      newPartitions.add(new DefaultPartition<BlockWriter>(partition.getPartitionedInstance()));
    }
    partitions.clear();

    List<BasicCounters<MutableLong>> deletedCounters = Lists.newArrayList();

    LOG.debug("block writer parallel partition count {}", context.getParallelPartitionCount());
    int morePartitionsToCreate = context.getParallelPartitionCount() - newPartitions.size();
    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<BlockWriter>> partitionIterator = newPartitions.iterator();

      while (morePartitionsToCreate++ < 0) {
        Partition<BlockWriter> toRemove = partitionIterator.next();
        deletedCounters.add(toRemove.getPartitionedInstance().fileCounters);

        partitionIterator.remove();
      }
    }
    else {
      //Add more partitions
      BlockWriter anOperator = newPartitions.iterator().next().getPartitionedInstance();

      while (morePartitionsToCreate-- > 0) {
        DefaultPartition<BlockWriter> partition = new DefaultPartition<BlockWriter>(anOperator);
        newPartitions.add(partition);
      }
    }

    //transfer the counters
    BlockWriter targetWriter = newPartitions.iterator().next().getPartitionedInstance();
    for (BasicCounters<MutableLong> removedCounter : deletedCounters) {
      addCounters(targetWriter.fileCounters, removedCounter);
    }
    LOG.debug("Block writers {}", newPartitions.size());
    return newPartitions;
  }

  /**
   * Transfers the counters in partitioning.
   *
   * @param target target counter
   * @param source removed counter
   */
  protected void addCounters(BasicCounters<MutableLong> target, BasicCounters<MutableLong> source)
  {
    for (Enum<BlockWriter.Counters> key : BlockWriter.Counters.values()) {
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
  public void partitioned(Map<Integer, Partition<BlockWriter>> partitions)
  {

  }
}
