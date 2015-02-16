/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;
/**
 * @author Yogi/Sandeep
 */

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.apps.ingestion.io.ftp.FTPBlockReader;
import com.datatorrent.apps.ingestion.io.output.HdfsFileMerger;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.fs.FileSplitter;

@ApplicationAnnotation(name="Ingestion")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileSplitter fileSplitter = dag.addOperator("FileSplitter", new FileSplitter());
    dag.setAttribute(fileSplitter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());
    
    BlockReader blockReader ;
    if("ftp".equals(conf.get("dt.operator.inputProtocol"))){
      blockReader = dag.addOperator("BlockReader", new FTPBlockReader());
    }
    else{
      blockReader = dag.addOperator("BlockReader", new BlockReader());
    }
    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR, new BlockReader.BlockReaderCountersAggregator());

    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    dag.setAttribute(blockWriter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BlockWriter.BlockWriterCountersAggregator());

    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    HdfsFileMerger merger = dag.addOperator("FileMerger", new HdfsFileMerger());
//    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);    
    dag.addStream("BlockData", blockReader.messages, blockWriter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput).setLocality(Locality.THREAD_LOCAL);
    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("MergeTrigger", synchronizer.trigger, /*console.input,*/ merger.processedFileInput);
  }

}
