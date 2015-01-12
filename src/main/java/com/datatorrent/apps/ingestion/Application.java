/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;
/**
 * @author Yogi/Sandeep
 */
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.BlockReader;
import com.datatorrent.apps.ingestion.io.input.FTPBlockReader;
import com.datatorrent.apps.ingestion.io.output.HdfsFileMerger;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.fs.FileSplitter;

@ApplicationAnnotation(name="Ingestion")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileSplitter fileSplitter = dag.addOperator("FileSplitter", new FileSplitter());
    
    BlockReader blockReader ;
    if("ftp".equals(conf.get("dt.operator.inputProtocol"))){
      blockReader = dag.addOperator("BlockReader", new FTPBlockReader());
    }
    else{
      blockReader = dag.addOperator("BlockReader", new BlockReader());
    }
    BlockWriter<ReaderRecord<Slice>> blockWriter = dag.addOperator("BlockWriter", new BlockWriter<ReaderRecord<Slice>>());
    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    HdfsFileMerger merger = dag.addOperator("FileMerger", new HdfsFileMerger());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);    
    dag.addStream("BlockData", blockReader.messages, blockWriter.input);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput);
    dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("MergeTrigger", synchronizer.trigger, console.input, merger.processedFileInput);
  }

}
