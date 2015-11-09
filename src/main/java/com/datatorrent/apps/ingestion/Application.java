/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

/**
 * <p>Application class.</p>
 *
 * @author Yogi/Sandeep
 * @since 1.0.0
 */
import com.datatorrent.apps.ingestion.io.jms.BytesNonAppendFileOutputOperator;
import com.datatorrent.apps.ingestion.io.s3.S3BytesFileOutputOperator;
import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.security.Key;
import java.util.Collection;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.lib.BandwidthPartitioner;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.common.metric.MetricsAggregator;
import com.datatorrent.common.metric.SingleMetricAggregator;
import com.datatorrent.common.metric.sum.LongSumAggregator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.contrib.splunk.SplunkStore;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.fs.FilterStreamProvider;
import com.datatorrent.lib.io.fs.FilterStreamProvider.FilterChainStreamProvider;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.apps.ingestion.io.BandwidthLimitingOperator;
import com.datatorrent.apps.ingestion.common.IngestionUtils;
import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.FilterStreamProviders;
import com.datatorrent.apps.ingestion.io.FilterStreamProviders.TimedCipherStreamProvider;
import com.datatorrent.apps.ingestion.io.ftp.FTPBlockReader;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.apps.ingestion.io.input.KafkaSinglePortByteArrayInputOperator;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.Scanner;
import com.datatorrent.apps.ingestion.io.input.SplunkBytesInputOperator;
import com.datatorrent.apps.ingestion.io.jms.BytesFileOutputOperator;
import com.datatorrent.apps.ingestion.io.jms.JMSBytesInputOperator;
import com.datatorrent.apps.ingestion.io.jms.JMSOutputOperator;
import com.datatorrent.apps.ingestion.io.output.FTPFileMerger;
import com.datatorrent.apps.ingestion.io.output.FTPOutputOperator;
import com.datatorrent.apps.ingestion.io.output.HDFSFileMerger;
import com.datatorrent.apps.ingestion.io.output.IngestionFileMerger;
import com.datatorrent.apps.ingestion.io.output.KafkaSinglePortByteOutputOperator;
import com.datatorrent.apps.ingestion.io.output.OutputFileMerger;
import com.datatorrent.apps.ingestion.io.output.SplunkBytesOutputOperator;
import com.datatorrent.apps.ingestion.io.s3.S3BlockReader;
import com.datatorrent.apps.ingestion.lib.AsymmetricKeyManager;
import com.datatorrent.apps.ingestion.lib.CryptoInformation;
import com.datatorrent.apps.ingestion.lib.PluginLoader;
import com.datatorrent.apps.ingestion.lib.PluginLoader.PluginInfo;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.apps.ingestion.process.CompressionFilterStream;
import com.datatorrent.apps.ingestion.process.CompressionFilterStream.CompressionFilterStreamProvider;
import com.datatorrent.apps.ingestion.process.compaction.MetaFileCreator;
import com.datatorrent.apps.ingestion.process.compaction.MetaFileWriter;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter;
import com.datatorrent.apps.ingestion.process.compaction.PartitionMetaDataEmitter.PatitionMetaData;

@ApplicationAnnotation(name = "Ingestion")
public class Application implements StreamingApplication
{
  public static final String AES_TRANSOFRMATION = "AES/ECB/PKCS5Padding";
  public static final String RSA_TRANSFORMATION = "RSA/ECB/PKCS1Padding";
  public static final String GZIP_FILE_EXTENSION = "gz";
  public static final String LZO_FILE_EXTENSION = "lzo";


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
      populateKafkaDAG(dag, conf, outputScheme, cryptoInformation);
      break;
    case JMS:
      populateJMSDAG(dag, conf, outputScheme, cryptoInformation);
      break;
    case SPLUNK:
      populateSplunkDAG(dag, conf, outputScheme, cryptoInformation);
      break;
    default:
      throw new IllegalArgumentException("scheme " + inputScheme + " is not supported.");
    }
  }

  private CryptoInformation getCryptoInformation(Configuration conf)
  {
    CryptoInformation cryptoInformation = null;
    if (isEncryptionEnabled(conf)) {
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
    fileSplitter.setInputScheme(inputScheme);
    setBandwidth(conf, fileSplitter);

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
    
    MetricsAggregator blockReaderMetrics = new MetricsAggregator();
    blockReaderMetrics.addAggregators("bytesReadPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
    dag.setAttribute(blockReader, Context.OperatorContext.METRICS_AGGREGATOR, blockReaderMetrics);

    if (conf.get("dt.application.Ingestion.inputReaders.count") != null) {
      dag.setAttribute(blockReader, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<BlockReader>(Integer.parseInt(conf.get("dt.application.Ingestion.inputReaders.count"))));
    }

    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    dag.setAttribute(blockWriter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    Tracker tracker = dag.addOperator("Tracker", new Tracker());
    boolean oneTimeCopy = conf.getBoolean("dt.input.oneTimeCopy", false);
    ((Scanner)fileSplitter.getScanner()).setOneTimeCopy(oneTimeCopy);
    tracker.setOneTimeCopy(oneTimeCopy);
    
    SummaryWriter summaryWriter = new SummaryWriter();
    dag.addOperator("SummaryWriter", summaryWriter);

    if (conf.getBoolean("dt.application.Ingestion.compress", false)) {
      if ("gzip".equalsIgnoreCase(conf.get("dt.application.Ingestion.compress.type"))) {
        fileSplitter.setcompressionExtension(GZIP_FILE_EXTENSION);
        blockWriter.setFilterStreamProvider(new FilterStreamProviders.TimedGZipFilterStreamProvider());
        //setting block size explicitly for gzip compression
        fileSplitter.setBlockSize(32*1024*1024l);
      } else if ("lzo".equalsIgnoreCase(conf.get("dt.application.Ingestion.compress.type"))) {
        CompressionFilterStream.CompressionFilterStreamProvider lzoProvider = getLzoProvider(conf);
        fileSplitter.setcompressionExtension(LZO_FILE_EXTENSION);
        blockWriter.setFilterStreamProvider(lzoProvider);
      }
    }

    dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput, tracker.inputFileSplitter);
    dag.addStream("FileSplitterTracker", fileSplitter.trackerOutPort, tracker.fileSplitterTracker);
    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
    dag.addStream("BlockData", blockReader.messages, blockWriter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput).setLocality(Locality.THREAD_LOCAL);
    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("SummaryLogs", tracker.trackerEventOutPort, summaryWriter.input);
    
    //Compaction is applicable only for file based input
    boolean compact = conf.getBoolean("output.compact", false);
    if(compact){
      //Emits metadata for blocks belonging to a partition
      PartitionMetaDataEmitter partitionMetaDataEmitter = dag.addOperator("PartitionMetaDataEmitter", new PartitionMetaDataEmitter());
      
      long partitionSizeInMB = conf.getLong("output.rolling_file_size", 1024);
      partitionMetaDataEmitter.setPartitionSizeInBytes(partitionSizeInMB*1024*1024);
      
      String seperator = conf.get("output.compact_separator", "");
      if(seperator != null){
        partitionMetaDataEmitter.setFileBoundarySeperator(StringEscapeUtils.unescapeJava(seperator));
      }
      
      //Writes partition files
      OutputFileMerger<PatitionMetaData> partitionWriter = dag.addOperator("PartitionWriter", new OutputFileMerger());
      //Waits for all partitions of a file
      MetaFileCreator metaFileCreator = dag.addOperator("MetaFileCreator", new MetaFileCreator());
      //Writes file entry into the MetaFile
      MetaFileWriter metaFileWriter = dag.addOperator("MetaFileWriter", new MetaFileWriter());
      
      String outputFilePath = conf.get("dt.operator.FileMerger.filePath", null);
      if(outputFilePath != null){
        partitionWriter.setFilePath(outputFilePath);
        metaFileWriter.setFilePath(outputFilePath);
      }
      
      //dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput);
      
      //Trigger from synchronizer indicating file is available for compaction (all blocks written to hdfs)
      dag.addStream("FileBlocksCompleteTrigger", synchronizer.trigger, partitionMetaDataEmitter.processedFileInput);
      //Metadata for blocks belonging to a partition 
      dag.addStream("PartitionMetaData", partitionMetaDataEmitter.patitionMetaDataOutputPort, partitionWriter.input);
      //Metadata for completedPartitions
      dag.addStream("CompletedPartitionMetaData", partitionWriter.completedFilesMetaOutput , metaFileCreator.partitionCompleteTrigger);
      //Info about file spanned across partitions
      dag.addStream("FilePartitionInfo", partitionMetaDataEmitter.filePartitionInfoOutputPort , metaFileCreator.filePartitionInfoPort);
      //Entry indicating start, end partition:offset
      dag.addStream("MetaFileEntry", metaFileCreator.indexEntryOuputPort, metaFileWriter.input);
      dag.addStream("MergerComplete", metaFileCreator.completedFilesMetaOutputPort, tracker.inputFileMerger);
      dag.addStream("MetaFileEntryTracker", metaFileCreator.trackerOutPort, tracker.mergerTracker);
    }
    else{
      //Use filemerger to replicate original source structure at destination if compaction is not enabled.
      IngestionFileMerger merger;
      switch (outputScheme) {
      case HDFS:
        if ("true".equalsIgnoreCase(conf.get("dt.output.enableFastMerge"))) {
          fileSplitter.setFastMergeEnabled(true);
          merger = new HDFSFileMerger();
          break;
        }
        merger = new IngestionFileMerger();
        break;
      case FTP:
        merger =new FTPFileMerger();
        break;
      default:
        merger = new IngestionFileMerger();
        merger.setWriteChecksum(false);
        break;
      }

      if (cryptoInformation != null) {
        merger.setEncrypt(true);
        merger.setCryptoInformation(cryptoInformation);
      }

      merger = dag.addOperator("FileMerger", merger);
      
      dag.addStream("MergeTrigger", synchronizer.trigger, merger.input);
      
      dag.addStream("MergerComplete", merger.completedFilesMetaOutput, tracker.inputFileMerger);
      dag.addStream("MergerSplitterTracker", merger.trackerOutPort, tracker.mergerTracker);
      
      MetricsAggregator mergerMetrics = new MetricsAggregator();
      mergerMetrics.addAggregators("bytesWrittenPerSec", new SingleMetricAggregator[]{new IngestionMaxAggregator()});
      dag.setAttribute(merger, Context.OperatorContext.METRICS_AGGREGATOR, mergerMetrics);
    }
    
    MetricsAggregator trackerAggregator = new MetricsAggregator();
    trackerAggregator.addAggregators("remainingFileCounts", new SingleMetricAggregator[]{new LongSumAggregator()});
    dag.setAttribute(tracker, Context.OperatorContext.METRICS_AGGREGATOR, trackerAggregator);

    String appId = dag.getValue(Context.DAGContext.APPLICATION_ID);
    IngestionUtils.createAppDataConnections(dag, appId + "_StatusCounts", "statusSchema.json", tracker.statusMetrics);
    IngestionUtils.createAppDataConnections(dag, appId + "_FileDetails", "fileDetailsSchema.json", tracker.fileDetailsMetrics);
  }

  private byte[] getKeyFromConfig(Configuration conf)
  {
    String encryptKey = conf.get("dt.application.Ingestion.encrypt.key");
    if (encryptKey != null && !encryptKey.isEmpty()) {
      return encryptKey.getBytes();
    }
    return null;
  }

  private CompressionFilterStreamProvider getLzoProvider(Configuration conf)
  {
    PluginInfo compressionPluginInfo = PluginLoader.discoverPlugin("compression", "lzo");
    String pluginJarPath = compressionPluginInfo.getJarName();
    String pluginClassName = compressionPluginInfo.getClassName();
    if (pluginClassName == null || pluginClassName.isEmpty()) {
      throw new RuntimeException("LZO compression output stream extending 'com.datatorrent.apps.ingestion.process.CompressionOutputStream' class not configured");
    }
    conf.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, conf.get(StramAppLauncher.LIBJARS_CONF_KEY_NAME) + "," + pluginJarPath);
    CompressionFilterStream.CompressionFilterStreamProvider lzoProvider = new CompressionFilterStream.CompressionFilterStreamProvider();
    lzoProvider.setCompressionClassName(pluginClassName);
    return lzoProvider;
  }

  /**
   * DAG for Kafka input source
   * 
   * @param dag
   * @param conf
   */
  private void populateKafkaDAG(DAG dag, Configuration conf, Scheme outputScheme, CryptoInformation cryptoInfo)
  {
    
    @SuppressWarnings("resource")
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();

    KafkaSinglePortByteArrayInputOperator inputOpr = dag.addOperator("MessageReader", new KafkaSinglePortByteArrayInputOperator());
    inputOpr.setConsumer(consumer);

    if (conf.get("dt.application.Ingestion.inputReaders.count") != null) {
      inputOpr.setInitialPartitionCount(Integer.parseInt(conf.get("dt.application.Ingestion.inputReaders.count")));
      inputOpr.setStrategy("ONE_TO_MANY");
      inputOpr.setRepartitionInterval(-1); // disabling dynamic partition
    }
    
    MetricsAggregator kafkaInputAggregator = new MetricsAggregator();
    kafkaInputAggregator.addAggregators("inputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
    kafkaInputAggregator.addAggregators("inputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
    dag.setAttribute(inputOpr, Context.OperatorContext.METRICS_AGGREGATOR, kafkaInputAggregator);

    BytesFileOutputOperator outputOpr = null;
    switch (outputScheme) {
    case HDFS:
    case FILE:
      if(isEncryptionEnabled(conf)) {
        outputOpr = dag.addOperator("FileWriter", BytesNonAppendFileOutputOperator.class);
      } else {
        outputOpr = dag.addOperator("FileWriter", new BytesFileOutputOperator());
      }
      break;
    case S3N:
      outputOpr = dag.addOperator("FileWriter", new S3BytesFileOutputOperator());
      break;
    case FTP:
      outputOpr = dag.addOperator("FileWriter", new FTPOutputOperator());
      break;
    case KAFKA:
      KafkaSinglePortByteOutputOperator output = dag.addOperator("MessageWriter", new KafkaSinglePortByteOutputOperator());
      dag.addStream("MessageData", inputOpr.outputPort, output.inputPort);

      MetricsAggregator kafkaOutputAggregator = new MetricsAggregator();
      kafkaOutputAggregator.addAggregators("outputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      kafkaOutputAggregator.addAggregators("outputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      dag.setAttribute(output, Context.OperatorContext.METRICS_AGGREGATOR, kafkaOutputAggregator);
      break;
    case JMS:
      JMSOutputOperator jmsOutput = dag.addOperator("MessageWriter", new JMSOutputOperator());
      dag.addStream("MessageData", inputOpr.outputPort, jmsOutput.inputPort);
      jmsOutput.setAckMode("AUTO_ACKNOWLEDGE");
      
      MetricsAggregator jmsOutputAggregator = new MetricsAggregator();
      jmsOutputAggregator.addAggregators("outputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      jmsOutputAggregator.addAggregators("outputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      dag.setAttribute(jmsOutput, Context.OperatorContext.METRICS_AGGREGATOR, jmsOutputAggregator);
      break;
    case SPLUNK:
      SplunkBytesOutputOperator splunkOutput = createSplunkOutputOperator(dag, conf);
      dag.addStream("MessageData", inputOpr.outputPort, splunkOutput.input);
      break;
    default:
      throw new IllegalArgumentException("scheme " + outputScheme + " is not supported.");
    }

    if(outputOpr == null) {
      return;
    }

    outputOpr.setFilePath(conf.get("dt.operator.FileMerger.filePath"));
    if(conf.get("dt.operator.MessageReader.prop.topic") != null) {
      outputOpr.setOutputFileNamePrefix(conf.get("dt.operator.MessageReader.prop.topic"));
    }
    
    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if (cryptoInfo != null) {
      TimedCipherStreamProvider cipherProvider = new TimedCipherStreamProvider(cryptoInfo.getTransformation(), cryptoInfo.getSecretKey());
      chainStreamProvider.addStreamProvider(cipherProvider);
    }

    setCompression(conf, chainStreamProvider, outputOpr);

    if (chainStreamProvider.getStreamProviders().size() > 0) {
      outputOpr.setFilterStreamProvider(chainStreamProvider);
    }
    setBandwidth(conf, inputOpr);
    dag.addStream("MessageData", inputOpr.outputPort, outputOpr.input);

    MetricsAggregator fileOutputaggregator = new MetricsAggregator();
    fileOutputaggregator.addAggregators("bytesPerSec", new SingleMetricAggregator[]{new IngestionMaxAggregator()});
    dag.setAttribute(outputOpr, Context.OperatorContext.METRICS_AGGREGATOR, fileOutputaggregator);
  }

  /**
   * DAG for JMS input source
   * 
   * @param dag
   * @param conf
   * @param cryptoInformation
   */
  public void populateJMSDAG(DAG dag, Configuration conf, Scheme outputScheme,CryptoInformation cryptoInfo)
  {
    // Reads from JMS
    JMSBytesInputOperator inputOpr = dag.addOperator("MessageReader", new JMSBytesInputOperator());

    if (conf.get("dt.application.Ingestion.inputReaders.count") != null) {
      if (conf.get("dt.application.Ingestion.bandwidth") != null) {
        dag.setAttribute(inputOpr, Context.OperatorContext.PARTITIONER, new BandwidthPartitioner<BandwidthLimitingOperator>(Integer.parseInt(conf.get("dt.application.Ingestion.inputReaders.count"))));
      } else {
        dag.setAttribute(inputOpr, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<JMSBytesInputOperator>(Integer.parseInt(conf.get("dt.application.Ingestion.inputReaders.count"))));
      }
    }

    MetricsAggregator jmsInputAggregator = new MetricsAggregator();
    jmsInputAggregator.addAggregators("inputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
    jmsInputAggregator.addAggregators("inputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
    dag.setAttribute(inputOpr, Context.OperatorContext.METRICS_AGGREGATOR, jmsInputAggregator);

    // Writes to file
    BytesFileOutputOperator outputOpr = null;
    switch (outputScheme) {
    case HDFS:
    case FILE:
      if(isEncryptionEnabled(conf)) {
        outputOpr = dag.addOperator("FileWriter", BytesNonAppendFileOutputOperator.class);
      } else {
        outputOpr = dag.addOperator("FileWriter", new BytesFileOutputOperator());
      }
      break;
    case S3N:
      outputOpr = dag.addOperator("FileWriter", new S3BytesFileOutputOperator());
      break;
    case FTP:
      outputOpr = dag.addOperator("FileWriter", new FTPOutputOperator());
      break;
    case KAFKA:
      KafkaSinglePortByteOutputOperator output = dag.addOperator("MessageWriter", new KafkaSinglePortByteOutputOperator());
      dag.addStream("MessageData", inputOpr.output, output.inputPort);
      
      MetricsAggregator kafkaOutputAggregator = new MetricsAggregator();
      kafkaOutputAggregator.addAggregators("outputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      kafkaOutputAggregator.addAggregators("outputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      dag.setAttribute(output, Context.OperatorContext.METRICS_AGGREGATOR, kafkaOutputAggregator);
      break;
    case JMS:
      JMSOutputOperator jmsOutput = dag.addOperator("MessageWriter", new JMSOutputOperator());
      jmsOutput.setAckMode("AUTO_ACKNOWLEDGE");
      dag.addStream("MessageData", inputOpr.output, jmsOutput.inputPort);
      
      MetricsAggregator jmsOutputAggregator = new MetricsAggregator();
      jmsOutputAggregator.addAggregators("outputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      jmsOutputAggregator.addAggregators("outputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      dag.setAttribute(jmsOutput, Context.OperatorContext.METRICS_AGGREGATOR, jmsOutputAggregator);
      break;
    case SPLUNK:
      SplunkBytesOutputOperator splunkOutput = createSplunkOutputOperator(dag, conf);
      dag.addStream("MessageData", inputOpr.output, splunkOutput.input);
      break;
    default:
      throw new IllegalArgumentException("scheme " + outputScheme + " is not supported.");
    }

    if(outputOpr == null) {
      return;
    }
    outputOpr.setFilePath(conf.get("dt.operator.FileMerger.filePath"));
    if(conf.get("dt.application.Ingestion.operator.MessageReader.prop.subject") != null) {
      outputOpr.setOutputFileNamePrefix(conf.get("dt.application.Ingestion.operator.MessageReader.prop.subject"));
    }
    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if (cryptoInfo != null) {
      TimedCipherStreamProvider cipherProvider = new TimedCipherStreamProvider(cryptoInfo.getTransformation(), cryptoInfo.getSecretKey());
      chainStreamProvider.addStreamProvider(cipherProvider);
    }

    setCompression(conf, chainStreamProvider, outputOpr);

    if (chainStreamProvider.getStreamProviders().size() > 0) {
      outputOpr.setFilterStreamProvider(chainStreamProvider);
    }
    setBandwidth(conf, inputOpr);
    // Stream connecting reader and writer
    dag.addStream("MessageData", inputOpr.output, outputOpr.input);
  }

  /**
   * DAG for Splunk input source
   *
   * @param dag
   * @param conf
   * @param cryptoInfo
   */
  public void populateSplunkDAG(DAG dag, Configuration conf, Scheme outputScheme,CryptoInformation cryptoInfo)
  {
    // Reads from Splunk
    SplunkBytesInputOperator inputOpr = dag.addOperator("MessageReader", new SplunkBytesInputOperator());
    SplunkStore store = new SplunkStore();
    store.setHost(conf.get("dt.application.Ingestion.operator.MessageReader.prop.host"));
    store.setPassword(conf.get("dt.application.Ingestion.operator.MessageReader.prop.password"));
    store.setPort(Integer.parseInt(conf.get("dt.application.Ingestion.operator.MessageReader.prop.port")));
    store.setUserName(conf.get("dt.application.Ingestion.operator.MessageReader.prop.userName"));
    inputOpr.setStore(store);
    if (conf.get("dt.application.Ingestion.inputReaders.count") != null) {
      if (conf.get("dt.application.Ingestion.bandwidth") != null) {
        dag.setAttribute(inputOpr, Context.OperatorContext.PARTITIONER, new BandwidthPartitioner<BandwidthLimitingOperator>(Integer.parseInt(conf.get("dt.application.Ingestion.inputReaders.count"))));
      } else {
        dag.setAttribute(inputOpr, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<SplunkBytesInputOperator>(Integer.parseInt(conf.get("dt.application.Ingestion.inputReaders.count"))));
      }
    }
    // Writes to file
    BytesFileOutputOperator outputOpr = null;
    switch (outputScheme) {
    case HDFS:
    case FILE:
      if(isEncryptionEnabled(conf)) {
        outputOpr = dag.addOperator("FileWriter", BytesNonAppendFileOutputOperator.class);
      } else {
        outputOpr = dag.addOperator("FileWriter", new BytesFileOutputOperator());
      }
      break;
    case FTP:
      outputOpr = dag.addOperator("FileWriter", new FTPOutputOperator());
      break;
    case KAFKA:
      KafkaSinglePortByteOutputOperator output = dag.addOperator("MessageWriter", new KafkaSinglePortByteOutputOperator());
      dag.addStream("MessageData", inputOpr.outputPort, output.inputPort);
      
      MetricsAggregator kafkaOutputAggregator = new MetricsAggregator();
      kafkaOutputAggregator.addAggregators("outputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      kafkaOutputAggregator.addAggregators("outputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      dag.setAttribute(output, Context.OperatorContext.METRICS_AGGREGATOR, kafkaOutputAggregator);
      break;
    case JMS:
      JMSOutputOperator jmsOutput = dag.addOperator("MessageWriter", new JMSOutputOperator());
      dag.addStream("MessageData", inputOpr.outputPort, jmsOutput.inputPort);
      
      MetricsAggregator jmsOutputAggregator = new MetricsAggregator();
      jmsOutputAggregator.addAggregators("outputMessagesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      jmsOutputAggregator.addAggregators("outputBytesPerSec", new SingleMetricAggregator[]{new LongSumAggregator()});
      dag.setAttribute(jmsOutput, Context.OperatorContext.METRICS_AGGREGATOR, jmsOutputAggregator);
      break;
    case SPLUNK:
      SplunkBytesOutputOperator splunkOutput = createSplunkOutputOperator(dag, conf);
      dag.addStream("MessageData", inputOpr.outputPort, splunkOutput.input);
      break;
    default:
      throw new IllegalArgumentException("scheme " + outputScheme + " is not supported.");
    }

    if(outputOpr == null) {
      return;
    }
    outputOpr.setFilePath(conf.get("dt.operator.FileMerger.filePath"));

    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if (cryptoInfo != null) {
      TimedCipherStreamProvider cipherProvider = new TimedCipherStreamProvider(cryptoInfo.getTransformation(), cryptoInfo.getSecretKey());
      chainStreamProvider.addStreamProvider(cipherProvider);
    }

    setCompression(conf, chainStreamProvider, outputOpr);

    if (chainStreamProvider.getStreamProviders().size() > 0) {
      outputOpr.setFilterStreamProvider(chainStreamProvider);
    }
    setBandwidth(conf, inputOpr);
    // Stream connecting reader and writer
    dag.addStream("MessageData", inputOpr.outputPort, outputOpr.input);
  }

  private void setCompression(Configuration conf, FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider, BytesFileOutputOperator outputOpr)
  {
    if (conf.getBoolean("dt.application.Ingestion.compress", false)) {
      if ("gzip".equalsIgnoreCase(conf.get("dt.application.Ingestion.compress.type"))) {
        outputOpr.setOutputFileExtension(GZIP_FILE_EXTENSION);
        chainStreamProvider.addStreamProvider(new FilterStreamProviders.TimedGZipFilterStreamProvider());
      } else if ("lzo".equalsIgnoreCase(conf.get("dt.application.Ingestion.compress.type"))) {
        CompressionFilterStream.CompressionFilterStreamProvider lzoProvider = getLzoProvider(conf);
        outputOpr.setOutputFileExtension(LZO_FILE_EXTENSION);
        chainStreamProvider.addStreamProvider(lzoProvider);
      }
    }
  }

  private void setBandwidth(Configuration conf, BandwidthLimitingOperator inputOpr)
  {
    long bandwidth = conf.getLong("dt.application.Ingestion.bandwidth", 0);
    if (bandwidth != 0) {
      inputOpr.getBandwidthManager().setBandwidth(bandwidth);
    }
  }

  private SplunkBytesOutputOperator createSplunkOutputOperator(DAG dag, Configuration conf)
  {
    SplunkBytesOutputOperator output = dag.addOperator("MessageWriter", new SplunkBytesOutputOperator());
    SplunkStore store = new SplunkStore();
    store.setHost(conf.get("dt.application.Ingestion.operator.MessageWriter.prop.host"));
    store.setPassword(conf.get("dt.application.Ingestion.operator.MessageWriter.prop.password"));
    store.setPort(Integer.parseInt(conf.get("dt.application.Ingestion.operator.MessageWriter.prop.port")));
    store.setUserName(conf.get("dt.application.Ingestion.operator.MessageWriter.prop.userName"));
    output.setStore(store);
    return output;
  }

  protected boolean isEncryptionEnabled(Configuration conf)
  {
    if(conf.getBoolean("dt.application.Ingestion.encrypt.aes", false) || conf.getBoolean("dt.application.Ingestion.encrypt.pki", false))
    {
      return true;
    }
    return false;
  }

  public static enum Scheme {
    FILE, FTP, S3N, HDFS, KAFKA, JMS, SPLUNK;

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
  
  public static class IngestionMaxAggregator implements SingleMetricAggregator, Serializable
  {
    private static final long serialVersionUID = 1L;

    public Object aggregate(Collection<Object> metricValues)
    {
      Long max = new Long(0);
      for (Object value : metricValues) {
        long lval = ((Number)value).longValue();
        if (max == null || max == 0 || lval > max) {
          max = lval;
        }
      }
      return max;
    }
    
  }

  public static final String ONE_TIME_COPY_DONE_FILE = "IngestionApp.complete";
}
