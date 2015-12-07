/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.module.io.fs;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.io.block.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.output.BlockWriter;
import com.datatorrent.lib.io.output.Synchronizer;
import com.datatorrent.lib.io.output.TrackerEvent;
import com.datatorrent.lib.io.input.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.netlet.util.Slice;

public class HDFSFileCopyModule implements Module
{

  @NotNull
  protected String hostName;
  @NotNull
  protected int port;
  @NotNull
  protected String directory;

  public final transient ProxyInputPort<FileMetadata> filesMetadataInput = new ProxyInputPort<FileMetadata>();
  public final transient ProxyInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new ProxyInputPort<BlockMetadata.FileBlockMetadata>();
  public final transient ProxyInputPort<ReaderRecord<Slice>> blockData = new ProxyInputPort<ReaderRecord<Slice>>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    //Defining DAG
    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);

    HDFSFileMerger merger = new HDFSFileMerger();
    merger = dag.addOperator("FileMerger", merger);
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

  public String getHostName()
  {
    return hostName;
  }

  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }

  public int getPort()
  {
    return port;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public String getDirectory()
  {
    return directory;
  }

  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  private String constructFilePath()
  {
    StringBuffer sb = new StringBuffer("hdfs://");
    sb.append(hostName);
    sb.append(":");
    sb.append(port);
    sb.append(directory);
    return sb.toString();
  }

  private static Logger LOG = LoggerFactory.getLogger(HDFSFileCopyModule.class);

}
