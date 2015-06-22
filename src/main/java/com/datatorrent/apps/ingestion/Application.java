/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

/**
 * @author Yogi/Sandeep
 */
import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.security.Key;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.FilterStreamProviders;
import com.datatorrent.apps.ingestion.io.FilterStreamProviders.TimedCipherStreamProvider;
import com.datatorrent.apps.ingestion.io.ftp.FTPBlockReader;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.Scanner;
import com.datatorrent.apps.ingestion.io.jms.BytesFileOutputOperator;
import com.datatorrent.apps.ingestion.io.jms.JMSBytesInputOperator;
import com.datatorrent.apps.ingestion.io.output.FTPFileMerger;
import com.datatorrent.apps.ingestion.io.output.FileMerger;
import com.datatorrent.apps.ingestion.io.output.HDFSFileMerger;
import com.datatorrent.apps.ingestion.io.s3.S3BlockReader;
import com.datatorrent.apps.ingestion.lib.AsymmetricKeyManager;
import com.datatorrent.apps.ingestion.lib.CryptoInformation;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.apps.ingestion.process.compaction.MetaFileCreator;
import com.datatorrent.apps.ingestion.process.compaction.MetaFileWriter;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter;
import com.datatorrent.apps.ingestion.process.compaction.PartitionWriter;
import com.datatorrent.apps.ingestion.process.LzoFilterStream;
import com.datatorrent.apps.ingestion.process.LzoFilterStream.LzoFilterStreamProvider;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.malhar.contrib.kafka.KafkaSinglePortByteArrayInputOperator;
import com.datatorrent.malhar.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.malhar.lib.io.fs.FilterStreamProvider;
import com.datatorrent.malhar.lib.io.fs.FilterStreamProvider.FilterChainStreamProvider;


@ApplicationAnnotation(name = "Ingestion")
public class Application implements StreamingApplication
{
  public static final String AES_TRANSOFRMATION = "AES/ECB/PKCS5Padding";
  public static final String RSA_TRANSFORMATION = "RSA/ECB/PKCS1Padding";
  public static final String GZIP_FILE_EXTENSION = "gz";
  public static final String LZO_FILE_EXTENSION = "lzo";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String inputSchemeStr = conf.get("dt.operator.BlockReader.prop.scheme");
    Scheme inputScheme = Scheme.valueOf(inputSchemeStr.toUpperCase());

    String outputSchemeStr = conf.get("dt.output.protocol");
    Scheme outputScheme = Scheme.valueOf(outputSchemeStr.toUpperCase());

    CryptoInformation cryptoInformation = getCryptoInformation(conf);

    switch (inputScheme) {
    case FILE:
    case FTP:
    case S3N:
    case HDFS:
      populateFileSourceDAG(dag, conf, inputScheme, outputScheme, cryptoInformation);
      break;
    case KAFKA:
      populateKafkaDAG(dag, conf, cryptoInformation);
      break;
    case JMS:
      populateJMSDAG(dag, conf, cryptoInformation);
      break;
    default:
      throw new IllegalArgumentException("scheme " + inputScheme + " is not supported.");
    }
  }

  private CryptoInformation getCryptoInformation(Configuration conf)
  {
    CryptoInformation cryptoInformation = null;
    if (conf.getBoolean("dt.application.Ingestion.encrypt.aes", false) || conf.getBoolean("dt.application.Ingestion.encrypt.pki", false)) {
      byte[] keyBytes = getKeyFromConfig(conf);
      if (conf.getBoolean("dt.application.Ingestion.encrypt.aes", false)) {
        Key secret = SymmetricKeyManager.getInstance().generateKey(keyBytes);
        cryptoInformation = new CryptoInformation(AES_TRANSOFRMATION, secret);
      } else if (conf.getBoolean("dt.application.Ingestion.encrypt.pki", false)) {
        Key secret = AsymmetricKeyManager.getInstance().generatePublicKey(keyBytes);
        cryptoInformation = new CryptoInformation(RSA_TRANSFORMATION, secret);
      }
    }
    return cryptoInformation;
  }

  /**
   * DAG for file based sources
   * 
   * @param dag
   * @param conf
   * @param inputScheme
   * @param cryptoInformation
   */
  private void populateFileSourceDAG(DAG dag, Configuration conf, Scheme inputScheme, Scheme outputScheme, CryptoInformation cryptoInformation)
  {
    IngestionFileSplitter fileSplitter = dag.addOperator("FileSplitter", new IngestionFileSplitter());
    dag.setAttribute(fileSplitter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockReader blockReader;
    switch (inputScheme) {
    case FTP:
      blockReader = dag.addOperator("BlockReader", new FTPBlockReader());
      break;
    case S3N:
      blockReader = dag.addOperator("BlockReader", new S3BlockReader());
      break;
    default:
      blockReader = dag.addOperator("BlockReader", new BlockReader(inputScheme));
    }
    blockReader.setUri(conf.get("dt.operator.FileSplitter.prop.scanner.files"));

    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    dag.setAttribute(blockWriter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    FileMerger merger;
    switch (outputScheme) {
    case HDFS:
      if ("true".equalsIgnoreCase(conf.get("dt.output.enableFastMerge"))) {
        fileSplitter.setFastMergeEnabled(true);
        merger = new HDFSFileMerger();
        break;
      }
      merger = new FileMerger();
      break;
    case FTP:
      merger =new FTPFileMerger();
      break;
    default:
      merger = new FileMerger();
      break;
    }

    if (cryptoInformation != null) {
      merger.setEncrypt(true);
      merger.setCryptoInformation(cryptoInformation);
    }

    if (conf.getBoolean("dt.application.Ingestion.compress.gzip", false)) {
      fileSplitter.setcompressionExtension(GZIP_FILE_EXTENSION);
      blockWriter.setFilterStreamProvider(new FilterStreamProviders.TimedGZipFilterStreamProvider());
    } else if (conf.getBoolean("dt.application.Ingestion.compress.lzo", false)) {
      LzoFilterStream.LzoFilterStreamProvider lzoProvider = getLzoProvider(conf);
      fileSplitter.setcompressionExtension(LZO_FILE_EXTENSION);
      blockWriter.setFilterStreamProvider(lzoProvider);
    }

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
    dag.addStream("BlockData", blockReader.messages, blockWriter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput).setLocality(Locality.THREAD_LOCAL);
    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    
    //Compaction is applicable only for file based input
    boolean compact = conf.getBoolean("dt.application.Ingestion.compact", false);
    if(compact){
      //Emits metadata for blocks belonging to a partition
      PartitionMetaDataEmitter partitionMetaDataEmitter = dag.addOperator("PartitionMetaDataEmitter", new PartitionMetaDataEmitter());
      String seperator = conf.get("dt.application.Ingestion.compact.separator", null);
      if(seperator != null){
        partitionMetaDataEmitter.setFileBoundarySeperator(StringEscapeUtils.unescapeJava(seperator));
      }
      
      //Writes partition files
      PartitionWriter partitionWriter = dag.addOperator("PartitionWriter", new PartitionWriter());
      //Waits for all partitions of a file
      MetaFileCreator metaFileCreator = dag.addOperator("MetaFileCreator", new MetaFileCreator());
      //Writes file entry into the MetaFile
      MetaFileWriter metaFileWriter = dag.addOperator("MetaFileWriter", new MetaFileWriter());
      
      dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput);
      
      //Trigger from synchronizer indicating file is available for compaction (all blocks written to hdfs)
      dag.addStream("FileBlocksCompleteTrigger", synchronizer.trigger, partitionMetaDataEmitter.processedFileInput);
      //Metadata for blocks belonging to a partition 
      dag.addStream("PartitionMetaData", partitionMetaDataEmitter.patitionMetaDataOutputPort, partitionWriter.input);
      //Metadata for completedPartitions
      dag.addStream("CompletedPartitionMetaData", partitionWriter.completedPartitionMetaDataOutputPort , metaFileCreator.partitionCompleteTrigger);
      //Info about file spanned across partitions
      dag.addStream("FilePartitionInfo", partitionMetaDataEmitter.filePartitionInfoOutputPort , metaFileCreator.filePartitionInfoPort);
      //Entry indicating start, end partition:offset
      dag.addStream("MetaFileEntry", metaFileCreator.indexEntryOuputPort, metaFileWriter.input);
    }
    else{
      //Use filemerge to replicate original source structure at destination if compaction is not enabled.
      merger = dag.addOperator("FileMerger", merger);
      
      Tracker tracker = dag.addOperator("Tracker", new Tracker());
      boolean oneTimeCopy = conf.getBoolean("dt.input.oneTimeCopy", false);
      ((Scanner)fileSplitter.getScanner()).setOneTimeCopy(oneTimeCopy);
      tracker.setOneTimeCopy(oneTimeCopy);
      
      dag.addStream("MergeTrigger", synchronizer.trigger, merger.input);
      dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput, tracker.inputFileSplitter);
      dag.addStream("MergerComplete", merger.output, tracker.inputFileMerger);
      
    }
  }

  private byte[] getKeyFromConfig(Configuration conf)
  {
    String encryptKey = conf.get("dt.application.Ingestion.encrypt.key");
    if (encryptKey != null && !encryptKey.isEmpty()) {
      return encryptKey.getBytes();
    }
    return null;
  }

  private LzoFilterStreamProvider getLzoProvider(Configuration conf)
  {
    String lzoStreamClassName = conf.get("dt.application.Ingestion.compress.lzo.className");
    if (lzoStreamClassName == null || lzoStreamClassName.isEmpty()) {
      throw new RuntimeException("LZO compression output stream extending 'com.datatorrent.apps.ingestion.process.LzoOutputStream' class not configured");
    }
    LzoFilterStream.LzoFilterStreamProvider lzoProvider = new LzoFilterStream.LzoFilterStreamProvider();
    lzoProvider.setCompressionClassName(lzoStreamClassName);
    return lzoProvider;
  }

  /**
   * DAG for Kafka input source
   * 
   * @param dag
   * @param conf
   */
  private void populateKafkaDAG(DAG dag, Configuration conf, CryptoInformation cryptoInfo)
  {
    
    @SuppressWarnings("resource")
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();

    KafkaSinglePortByteArrayInputOperator inputOpr = dag.addOperator("MessageReader", new KafkaSinglePortByteArrayInputOperator());
    inputOpr.setConsumer(consumer);

    BytesFileOutputOperator outputOpr = dag.addOperator("FileWriter", new BytesFileOutputOperator());
    outputOpr.setFilePath(conf.get("dt.operator.FileMerger.prop.filePath"));
    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if (cryptoInfo != null) {
      TimedCipherStreamProvider cipherProvider = new TimedCipherStreamProvider(cryptoInfo.getTransformation(), cryptoInfo.getSecretKey());
      chainStreamProvider.addStreamProvider(cipherProvider);
    }

    setCompressionForMessageSource(conf, chainStreamProvider, outputOpr);


    if (chainStreamProvider.getStreamProviders().size() > 0) {
      outputOpr.setFilterStreamProvider(chainStreamProvider);
    }

    dag.addStream("MessageData", inputOpr.outputPort, outputOpr.input);

  }

  /**
   * DAG for JMS input source
   * 
   * @param dag
   * @param conf
   * @param cryptoInformation
   */
  public void populateJMSDAG(DAG dag, Configuration conf, CryptoInformation cryptoInfo)
  {
    // Reads from JMS
    JMSBytesInputOperator inputOpr = dag.addOperator("MessageReader", new JMSBytesInputOperator());
    // Writes to file
    BytesFileOutputOperator outputOpr = dag.addOperator("FileWriter", new BytesFileOutputOperator());
    outputOpr.setFilePath(conf.get("dt.operator.FileMerger.prop.filePath"));

    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if (cryptoInfo != null) {
      TimedCipherStreamProvider cipherProvider = new TimedCipherStreamProvider(cryptoInfo.getTransformation(), cryptoInfo.getSecretKey());
      chainStreamProvider.addStreamProvider(cipherProvider);
    }

    setCompressionForMessageSource(conf, chainStreamProvider, outputOpr);


    if (chainStreamProvider.getStreamProviders().size() > 0) {
      outputOpr.setFilterStreamProvider(chainStreamProvider);
    }

    // Stream connecting reader and writer
    dag.addStream("MessageData", inputOpr.output, outputOpr.input);
  }

  private void setCompressionForMessageSource(Configuration conf, FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider, BytesFileOutputOperator outputOpr)
  {
    if (conf.getBoolean("dt.application.Ingestion.compress.gzip", false)) {
      outputOpr.setOutputFileExtension(GZIP_FILE_EXTENSION);
      chainStreamProvider.addStreamProvider(new FilterStreamProviders.TimedGZipFilterStreamProvider());
    } else if (conf.getBoolean("dt.application.Ingestion.compress.lzo", false)) {
      LzoFilterStream.LzoFilterStreamProvider lzoProvider = getLzoProvider(conf);
      outputOpr.setOutputFileExtension(LZO_FILE_EXTENSION);
      chainStreamProvider.addStreamProvider(lzoProvider);
    }
  }

  

  public static enum Scheme {
    FILE, FTP, S3N, HDFS, KAFKA, JMS;

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString()
    {
      return super.toString().toLowerCase();
    }

  }

  public static final String ONE_TIME_COPY_DONE_FILE = "IngestionApp.complete";
}
