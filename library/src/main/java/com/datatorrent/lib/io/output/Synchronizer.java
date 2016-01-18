/*
 * Copyright (c) 2016 DataTorrent, Inc. 
 * ALL Rights Reserved.
 *
 */
package com.datatorrent.lib.io.output;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.input.FileSplitterInput;
import com.datatorrent.lib.io.input.ModuleFileSplitter.ModuleFileMetaData;

/**
 * <p>
 * Synchronizer class.
 * </p>
 *
 */
public class Synchronizer extends BaseOperator
{
  private Map<String, Set<Long>> fileToActiveBlockMap = Maps.newHashMap();
  private Map<String, Set<Long>> fileToCompetedBlockMap = Maps.newHashMap();
  private Map<String, ExtendedModuleFileMetaData> fileMetadataMap = Maps.newHashMap();
  private final BasicCounters<MutableLong> counters;
  private transient Context.OperatorContext context;

  public Synchronizer()
  {
    counters = new BasicCounters<MutableLong>(MutableLong.class);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    this.context = context;
    counters.setCounter(FileProcessingCounters.NUM_OF_FILES, new MutableLong());
    counters.setCounter(FileProcessingCounters.PROCESSING_TIME, new MutableLong());
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

  public final transient DefaultInputPort<FileSplitterInput.FileMetadata> filesMetadataInput = new DefaultInputPort<FileSplitterInput.FileMetadata>()
  {
    @Override
    public void process(FileSplitterInput.FileMetadata fmd)
    {
      ExtendedModuleFileMetaData fileMetadata = null;

      if (fmd instanceof ModuleFileMetaData) {
        fileMetadata = new ExtendedModuleFileMetaData((ModuleFileMetaData)fmd);
      }

      if (null == fileMetadata) {
        throw new RuntimeException("Input tuple is not an instance of IngestionFileMetaData.");
      }

      String filePath = fileMetadata.getFilePath();
      Set<Long> activeBlocks = Sets.newHashSet();
      long[] blockIds = fileMetadata.getBlockIds();
      LOG.debug("received file {} with total number of blocks {} with blockIds {}", filePath,
          fileMetadata.getNumberOfBlocks(), Arrays.toString(blockIds));
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
        counters.getCounter(FileProcessingCounters.NUM_OF_FILES).increment();
        counters.getCounter(FileProcessingCounters.PROCESSING_TIME).add(fileProcessingTime);
        trigger.emit(fileMetadata);
        LOG.debug("Total time taken to process the file {} is {} ms", fileMetadata.getFilePath(), fileProcessingTime);
      } else {
        fileMetadataMap.put(filePath, fileMetadata);
        fileToActiveBlockMap.put(filePath, activeBlocks);
      }

    }
  };

  public final transient DefaultInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new DefaultInputPort<BlockMetadata.FileBlockMetadata>()
  {
    @Override
    public void process(BlockMetadata.FileBlockMetadata blockMetadata)
    {
      String filePath = blockMetadata.getFilePath();
      LOG.debug("received blockId {} for file {}", blockMetadata.getBlockId(), filePath);
      Set<Long> activeBlocks = fileToActiveBlockMap.get(filePath);
      if (activeBlocks != null) {
        activeBlocks.remove(blockMetadata.getBlockId());
        ModuleFileMetaData ingestionFileMetaData = fileMetadataMap.get(filePath);
        //TODO: enable compression counters
        //ingestionFileMetaData.setCompressionTime(ingestionFileMetaData.getCompressionTime() + blockMetadata.getCompressionTime());
        //ingestionFileMetaData.setOutputFileSize(ingestionFileMetaData.getOutputFileSize() + blockMetadata.getCompressedSize());
        if (activeBlocks.isEmpty()) {
          ExtendedModuleFileMetaData fileMetadata = fileMetadataMap.remove(filePath);
          long fileProcessingTime = System.currentTimeMillis() - fileMetadata.getDiscoverTime();
          counters.getCounter(FileProcessingCounters.NUM_OF_FILES).increment();
          counters.getCounter(FileProcessingCounters.PROCESSING_TIME).add(fileProcessingTime);
          trigger.emit(fileMetadata);
          LOG.debug("Total time taken to process the file {} is {} ms", fileMetadata.getFilePath(), fileProcessingTime);
          fileToActiveBlockMap.remove(filePath);
        }
      } else {
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
  public final transient DefaultOutputPort<ExtendedModuleFileMetaData> trigger = new DefaultOutputPort<ExtendedModuleFileMetaData>();

  public static enum FileProcessingCounters
  {
    PROCESSING_TIME, NUM_OF_FILES
  }
}
