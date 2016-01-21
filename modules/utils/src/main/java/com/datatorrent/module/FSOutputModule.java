/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.io.block.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.input.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.lib.io.output.BlockWriter;
import com.datatorrent.lib.io.output.IngestionFileMerger;
import com.datatorrent.lib.io.output.Synchronizer;
import com.datatorrent.lib.io.output.TrackerEvent;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.netlet.util.Slice;

/**
 * FSOutputModule is an abstract module for file system output modules like HDFS, S3, NFS, etc.
 * It writes files and fetches FileMetadata, BlockMetadata and the block bytes from upstream operator.&nbsp;
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: 3 input ports for FileMetadata, BlockMetadata and block bytes.<br>
 * <b>Output</b>: No proxy output port. <br>
 * <br>
 * Properties:<br>
 * <b>directory</b>: Specify the destination directory. <br>
 * <br>
 * <\p>
 */
public abstract class FSOutputModule implements Module
{
  @NotNull
  protected String directory;

  public final transient ProxyInputPort<FileMetadata> filesMetadataInput = new ProxyInputPort<FileMetadata>();
  public final transient ProxyInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new ProxyInputPort<BlockMetadata.FileBlockMetadata>();
  public final transient ProxyInputPort<ReaderRecord<Slice>> blockData = new ProxyInputPort<ReaderRecord<Slice>>();

  public abstract IngestionFileMerger getFileMerger();

  public BlockWriter getBlockWriter()
  {
    return new BlockWriter();
  }

  public Synchronizer getSynchronizer()
  {
    return new Synchronizer();
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Defining DAG
    BlockWriter blockWriter = dag.addOperator("BlockWriter", getBlockWriter());
    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", getSynchronizer());

    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);

    IngestionFileMerger merger = dag.addOperator("FileMerger", getFileMerger());
    dag.addStream("MergeTrigger", synchronizer.trigger, merger.input);

    DevNull<ModuleFileSplitter.ModuleFileMetaData> devNull1 = dag.addOperator("devNull1", DevNull.class);
    dag.addStream("ignored", merger.completedFilesMetaOutput, devNull1.data);

    DevNull<TrackerEvent> devNull2 = dag.addOperator("devNull2", DevNull.class);
    dag.addStream("ignored2", merger.trackerOutPort, devNull2.data);

    //Setting operator properties
    merger.setFilePath(constructFilePath());

    //Binding proxy ports
    filesMetadataInput.set(synchronizer.filesMetadataInput);
    blocksMetadataInput.set(blockWriter.blockMetadataInput);
    blockData.set(blockWriter.input);

  }

  /**
   * Return the directory
   * @return the destination directory
   */
  public String getDirectory()
  {
    return directory;
  }

  /**
   * Sets the destination directory
   * @param directory directory
   */
  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  /**
   * Constructs the file path
   * @return
   */
  protected abstract String constructFilePath();


  private static Logger LOG = LoggerFactory.getLogger(FSOutputModule.class);
}

