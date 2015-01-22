/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.apps.ingestion;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.datatorrent.lib.io.fs.FileSplitter.BlockMetadata;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author Yogi/Sandeep
 */
public class Synchronizer extends BaseOperator
{
  private Map<String, Set<Long>> fileToActiveBlockMap = Maps.newHashMap();
  private Map<String, Set<Long>> fileToCompetedBlockMap = Maps.newHashMap();
  private Map<String, FileSplitter.FileMetadata> fileMetadataMap = Maps.newHashMap();
  private final BasicCounters<MutableLong> counters;
  private final MutableLong fileCount;
  private final MutableLong processingTime;
  private transient Context.OperatorContext context;

  public Synchronizer()
  {
    counters = new BasicCounters<MutableLong>(MutableLong.class);
    fileCount = new MutableLong();
    processingTime = new MutableLong();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    this.context = context;
    counters.setCounter(FileProcessingCounters.NUM_OF_FILES, fileCount);
    counters.setCounter(FileProcessingCounters.PROCESSING_TIME, processingTime);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    context.setCounters(counters);
  }

  public final transient DefaultInputPort<FileSplitter.FileMetadata> filesMetadataInput = new DefaultInputPort<FileSplitter.FileMetadata>()
  {
    @Override
    public void process(FileSplitter.FileMetadata fileMetadata)
    {
      String filePath = fileMetadata.getFilePath();
      Set<Long> activeBlocks = Sets.newHashSet();
      long[] blockIds = fileMetadata.getBlockIds();
      LOG.debug("received file {} with total number of blocks {} with blockIds {}", filePath, fileMetadata.getNumberOfBlocks(), Arrays.toString(blockIds));
      for (int i = 0; i < fileMetadata.getNumberOfBlocks(); i++) {
        activeBlocks.add(blockIds[i]);
      }
      Set<Long> completedBlocks = fileToCompetedBlockMap.get(filePath);
      if (completedBlocks != null) {
        fileToCompetedBlockMap.remove(filePath);
        for (Long blockId : completedBlocks) {
          activeBlocks.remove(blockId);
        }
      }
      if (activeBlocks.isEmpty()) {
        long fileProcessingTime = System.currentTimeMillis() - fileMetadata.getDiscoverTime();
        fileCount.increment();
        processingTime.add(fileProcessingTime);
        trigger.emit(fileMetadata);
        LOG.debug("Total time taken to process the file {} is {} ms", fileMetadata.getFilePath(), fileProcessingTime);
      }
      else {
        fileMetadataMap.put(filePath, fileMetadata);
        fileToActiveBlockMap.put(filePath, activeBlocks);
      }

    }
  };

  public final transient DefaultInputPort<BlockMetadata> blocksMetadataInput = new DefaultInputPort<BlockMetadata>()
  {
    @Override
    public void process(BlockMetadata blockMetadata)
    {
      String filePath = blockMetadata.getFilePath();
      LOG.debug("received blockId {} for file {}", blockMetadata.getBlockId(), filePath);
      Set<Long> activeBlocks = fileToActiveBlockMap.get(filePath);
      if (activeBlocks != null) {
        activeBlocks.remove(blockMetadata.getBlockId());
        if (activeBlocks.isEmpty()) {
          FileSplitter.FileMetadata fileMetadata = fileMetadataMap.remove(filePath);
          long fileProcessingTime = System.currentTimeMillis() - fileMetadata.getDiscoverTime();
          fileCount.increment();
          processingTime.add(fileProcessingTime);
          trigger.emit(fileMetadata);
          LOG.debug("Total time taken to process the file {} is {} ms", fileMetadata.getFilePath(), fileProcessingTime);
          fileToActiveBlockMap.remove(filePath);
        }
      }
      else {
        Set<Long> completedBlocks = fileToCompetedBlockMap.get(filePath);
        if (completedBlocks == null) {
          completedBlocks = Sets.newHashSet();
          fileToCompetedBlockMap.put(filePath, completedBlocks);
        }
        completedBlocks.add(blockMetadata.getBlockId());
      }
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(Synchronizer.class);
  public final transient DefaultOutputPort<FileSplitter.FileMetadata> trigger = new DefaultOutputPort<FileSplitter.FileMetadata>();

  public static enum FileProcessingCounters
  {
    PROCESSING_TIME, NUM_OF_FILES
  }
}
