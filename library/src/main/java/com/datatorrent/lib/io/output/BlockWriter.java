/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.lib.io.output;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.lib.io.output.FilterStreamProviders;
import com.datatorrent.lib.io.output.FilterStreamProviders.TimedGZIPOutputStream;
import com.datatorrent.lib.io.output.FilterStreamProviders.TimedGZipFilterStreamProvider;
import com.datatorrent.lib.io.output.CompressionFilterStream.CompressionFilterStreamProvider;
import com.datatorrent.lib.io.output.CompressionFilterStream.TimedCompressionOutputStream;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.netlet.util.Slice;
import com.google.common.collect.Lists;

/**
 * Writes a block to the fs.
 *
 * @author Yogi/Sandeep
 * @since 1.0.0
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

    setFilterStreamTimingCounters();
    
    streamsCache.asMap().clear();
    endOffsets.clear();

    for (BlockMetadata.FileBlockMetadata blockMetadata : blockMetadatas) {
      try {
        finalizeFile(Long.toString(blockMetadata.getBlockId()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      blockMetadataOutput.emit(blockMetadata);
    }
    blockMetadatas.clear();
  }

  private void setFilterStreamTimingCounters()
  {
    if(filterStreamProvider != null){
      for (BlockMetadata.FileBlockMetadata blockMetadata : blockMetadatas) {
        FilterOutputStream filterStream;
        try {
          filterStream = ((FilterStreamContext<FilterOutputStream>)streamsCache.get(Long.toString(blockMetadata.getBlockId()))).getFilterStream();
        } catch (ExecutionException e) {
          throw new RuntimeException();
        }
        long timeTakenNanos = 0;
        if ((filterStream != null) && (filterStream instanceof TimedGZIPOutputStream)) {
          TimedGZIPOutputStream stream = (TimedGZIPOutputStream) filterStream;
          timeTakenNanos += stream.getStreamTimeNanos();
        }
        else if ((filterStream != null) && (filterStream instanceof TimedCompressionOutputStream)) {
          TimedCompressionOutputStream stream = (TimedCompressionOutputStream) filterStream;
          timeTakenNanos += stream.getStreamTimeNanos();
        }
        //TODO: Uncomment following line and enable compressionTime field over 
        //blockMetaData when supporting compression. 
        //blockMetadata.setCompressionTime(timeTakenNanos);
      }
    }
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

    // if there is no change of count, return the same collection
    if(context.getParallelPartitionCount() == partitions.size()){
      LOG.debug("no change is partition count: " + partitions.size());
      return partitions;
    }

    List<BasicCounters<MutableLong>> deletedCounters = Lists.newArrayList();

    LOG.debug("block writer parallel partition count {}", context.getParallelPartitionCount());
    int morePartitionsToCreate = context.getParallelPartitionCount() - partitions.size();
    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<BlockWriter>> partitionIterator = partitions.iterator();

      while (morePartitionsToCreate++ < 0) {
        Partition<BlockWriter> toRemove = partitionIterator.next();
        deletedCounters.add(toRemove.getPartitionedInstance().fileCounters);
        partitionIterator.remove();
      }
    }
    else {
      //Add more partitions
      BlockWriter anOperator = partitions.iterator().next().getPartitionedInstance();

      while (morePartitionsToCreate-- > 0) {
        DefaultPartition<BlockWriter> partition = new DefaultPartition<BlockWriter>(anOperator);
        partitions.add(partition);
      }
    }

    //transfer the counters
    BlockWriter targetWriter = partitions.iterator().next().getPartitionedInstance();
    for (BasicCounters<MutableLong> removedCounter : deletedCounters) {
      addCounters(targetWriter.fileCounters, removedCounter);
    }
    LOG.debug("Block writers {}", partitions.size());
    return partitions;
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
