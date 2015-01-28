/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion.io;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.io.fs.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.io.fs.FileSplitter;

/**
 * Writes a block to the fs.
 *
 * @author Yogi/Sandeep
 */
public class BlockWriter extends AbstractFileOutputOperator<ReaderRecord<Slice>>
{
  private transient List<FileSplitter.BlockMetadata> blockMetadatas;

  public final transient DefaultInputPort<FileSplitter.BlockMetadata> blockMetadataInput = new DefaultInputPort<FileSplitter.BlockMetadata>()
  {
    @Override
    public void process(FileSplitter.BlockMetadata blockMetadata)
    {
      blockMetadatas.add(blockMetadata);
      LOG.debug("received blockId {} for file {} ", blockMetadata.getBlockId(), blockMetadata.getFilePath());
    }
  };

  public final transient DefaultOutputPort<FileSplitter.BlockMetadata> blockMetadataOutput = new DefaultOutputPort<FileSplitter.BlockMetadata>();

  public BlockWriter()
  {
    super();
    blockMetadatas = Lists.newArrayList();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    filePath = filePath + "/" + context.getValue(DAG.APPLICATION_ID);
    super.setup(context);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    streamsCache.asMap().clear();
    endOffsets.clear();
    for (FileSplitter.BlockMetadata blockMetadata : blockMetadatas) {
      blockMetadataOutput.emit(blockMetadata);
    }
    blockMetadatas.clear();
  }

  @Override
  protected String getFileName(ReaderRecord<Slice> tuple)
  {
    return Long.toString(tuple.getBlockId());
  }

  @Override
  protected byte[] getBytesForTuple(ReaderRecord<Slice> tuple)
  {
    return tuple.getRecord().buffer;
  }

  private static final Logger LOG = LoggerFactory.getLogger(BlockWriter.class);
}
