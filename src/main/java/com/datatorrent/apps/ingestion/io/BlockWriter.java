/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io;

import java.io.File;
import java.util.List;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Writes a block to the fs.
 *
 * @author Yogi/Sandeep
 */
public class BlockWriter extends AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>>
{
  public static final String SUBDIR_BLOCKS = "blocks";
  private transient List<BlockMetadata.FileBlockMetadata> blockMetadatas;

  private transient long timePerWindow;

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
    filePath = context.getValue(Context.DAGContext.APPLICATION_PATH) + File.separator + SUBDIR_BLOCKS;
    super.setup(context);
    fileCounters.setCounter(BlockKeys.BLOCKS, new MutableLong());
    fileCounters.setCounter(BlockKeys.WRITE_TIME_WINDOW, new MutableLong());
  }

  @Override
  protected void processTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    long start = System.currentTimeMillis();
    super.processTuple(tuple);
    timePerWindow += System.currentTimeMillis() - start;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    streamsCache.asMap().clear();
    endOffsets.clear();
    fileCounters.getCounter(BlockKeys.BLOCKS).add(blockMetadatas.size());

    for (BlockMetadata.FileBlockMetadata blockMetadata : blockMetadatas) {
      blockMetadataOutput.emit(blockMetadata);
    }
    blockMetadatas.clear();
    fileCounters.getCounter(BlockKeys.WRITE_TIME_WINDOW).setValue(timePerWindow);
    timePerWindow = 0;
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

  protected static enum BlockKeys
  {
    BLOCKS, WRITE_TIME_WINDOW
  }
}
