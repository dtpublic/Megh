/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

/**
 * @author Yogi/Sandeep
 */

import java.util.Properties;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.ftp.FTPBlockReader;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.apps.ingestion.io.output.FileMerger;
import com.datatorrent.apps.ingestion.io.output.HDFSFileMerger;
import com.datatorrent.apps.ingestion.io.s3.S3BlockReader;
import com.datatorrent.apps.ingestion.kafka.FileOutputOperator;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "Ingestion")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String schemeStr = conf.get("dt.operator.BlockReader.prop.scheme");
    Scheme scheme = Scheme.valueOf(schemeStr.toUpperCase());

    switch (scheme) {
    case FILE:
    case FTP:
    case S3N:
    case HDFS:
      populateFileSourceDAG(dag, conf, scheme);
      break;
    case KAFKA:
    case JMS:
      populateMessageSourceDAG(dag, conf);
      break;
    default:
      throw new IllegalArgumentException("scheme " + scheme + " is not supported.");
    }
  }

  /**
   * DAG for file based sources
   * 
   * @param dag
   * @param conf
   * @param scheme 
   */
  private void populateFileSourceDAG(DAG dag, Configuration conf, Scheme scheme)
  {
    IngestionFileSplitter fileSplitter = dag.addOperator("FileSplitter", new IngestionFileSplitter());
    dag.setAttribute(fileSplitter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockReader blockReader;
    switch (scheme) {
    case FTP:
      blockReader = dag.addOperator("BlockReader", new FTPBlockReader());
      break;
    case S3N:
      blockReader = dag.addOperator("BlockReader", new S3BlockReader());
      break;
    default:
      blockReader = dag.addOperator("BlockReader", new BlockReader(scheme));
    }
    
    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    dag.setAttribute(blockWriter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    FileMerger merger;
    String outputSchemeStr = conf.get("dt.output.protocol");
    Scheme outputScheme = Scheme.valueOf(outputSchemeStr.toUpperCase());
    
    if ((Scheme.HDFS == outputScheme) && "true".equalsIgnoreCase(conf.get("dt.output.enableFastMerge"))) {
      fileSplitter.setFastMergeEnabled(true);
      merger = dag.addOperator("FileMerger", new HDFSFileMerger());
    } else {
      merger = dag.addOperator("FileMerger", new FileMerger());
    }

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
    dag.addStream("BlockData", blockReader.messages, blockWriter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput).setLocality(Locality.THREAD_LOCAL);
    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("MergeTrigger", synchronizer.trigger, merger.input);
    dag.addStream("MergerComplete", merger.output, console.input);
  }

  /**
   * DAG for message based sources
   * 
   * @param dag
   * @param conf
   */
  private void populateMessageSourceDAG(DAG dag, Configuration conf)
  {
    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "main_group");
    
    @SuppressWarnings("resource")
    HighlevelKafkaConsumer consumer = new HighlevelKafkaConsumer(props);
    
    KafkaSinglePortStringInputOperator inputOpr = dag.addOperator("MessageReader", new KafkaSinglePortStringInputOperator());
    inputOpr.setConsumer(consumer);

    FileOutputOperator outputOpr = dag.addOperator("fileWriter", new FileOutputOperator());
    dag.addStream("kafkaData", inputOpr.outputPort, outputOpr.input);
  }

  public static enum Scheme {
    FILE{
      @Override
      public String toString()
      {
        return "file";
      }
    },
    FTP{
      @Override
      public String toString()
      {
        return "ftp";
      }
    }, 
    S3N{
      @Override
      public String toString()
      {
        return "s3n";
      }
    }, HDFS{
      @Override
      public String toString()
      {
        return "hdfs";
      }
    }, KAFKA{
      @Override
      public String toString()
      {
        return "kafka";
      }
    },
    JMS{
      @Override
      public String toString()
      {
        return "jms";
      }
    }
  }

}
