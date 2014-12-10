/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.output.HdfsFileMerger;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.datatorrent.lib.io.fs.FixedBytesBlockReader;

@ApplicationAnnotation(name="Ingestion")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileSplitter fileSplitter = dag.addOperator("File-splitter", new FileSplitter());
    FixedBytesBlockReader blockReader = dag.addOperator("Block-reader", new FixedBytesBlockReader());

    Synchronizer synchronizer = dag.addOperator("Synchronizer", new Synchronizer());
    BlockWriter<ReaderRecord<Slice>> blockWriter = dag.addOperator("Writer", new BlockWriter<ReaderRecord<Slice>>());

    HdfsFileMerger merger = dag.addOperator("Merger", new HdfsFileMerger());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("Block-metadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);    
    dag.addStream("Block-Data", blockReader.messages, blockWriter.input);
    dag.addStream("Processed-blockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput);
    dag.addStream("Completed-blockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("MergeTrigger", synchronizer.trigger, console.input, merger.processedFileInput);
  }
}
