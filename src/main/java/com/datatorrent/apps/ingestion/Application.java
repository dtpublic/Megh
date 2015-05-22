/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

/**
 * @author Yogi/Sandeep
 */
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.util.Properties;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

import org.apache.commons.io.IOUtils;
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
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.Scanner;
import com.datatorrent.apps.ingestion.io.jms.BytesFileOutputOperator;
import com.datatorrent.apps.ingestion.io.jms.JMSBytesInputOperator;
import com.datatorrent.apps.ingestion.io.output.FTPFileMerger;
import com.datatorrent.apps.ingestion.io.output.FileMerger;
import com.datatorrent.apps.ingestion.io.output.HDFSFileMerger;
import com.datatorrent.apps.ingestion.io.s3.S3BlockReader;
import com.datatorrent.apps.ingestion.lib.AsymmetricKeyManager;
import com.datatorrent.apps.ingestion.lib.CipherProvider;
import com.datatorrent.apps.ingestion.lib.CryptoInformation;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.contrib.kafka.KafkaSinglePortByteArrayInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.fs.FilterStreamCodec;
import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.lib.io.fs.FilterStreamProvider;

@ApplicationAnnotation(name = "Ingestion")
public class Application implements StreamingApplication
{
  private final String AES_TRANSOFRMATION = "AES/ECB/PKCS5Padding";
  private final String RSA_TRANSFORMATION = "RSA/ECB/PKCS1Padding";

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

    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    dag.setAttribute(blockWriter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    FileMerger merger;
    switch (outputScheme) {
    case HDFS:
      if ("true".equalsIgnoreCase(conf.get("dt.output.enableFastMerge"))) {
        fileSplitter.setFastMergeEnabled(true);
        merger = dag.addOperator("FileMerger", new HDFSFileMerger());
        break;
      }
      merger = dag.addOperator("FileMerger", new FileMerger());
      break;
    case FTP:
      merger = dag.addOperator("FileMerger", new FTPFileMerger());
      break;
    default:
      merger = dag.addOperator("FileMerger", new FileMerger());
      break;
    }

    if (cryptoInformation != null) {
      merger.setEncrypt(true);
      merger.setCryptoInformation(cryptoInformation);
    }

    if (conf.getBoolean("dt.application.Ingestion.compress", false)) {
      blockWriter.setFilterStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());
    }

    Tracker tracker = dag.addOperator("Tracker", new Tracker());
    boolean oneTimeCopy = conf.getBoolean("dt.input.oneTimeCopy", false);
    ((Scanner)fileSplitter.getScanner()).setOneTimeCopy(oneTimeCopy);
    tracker.setOneTimeCopy(oneTimeCopy);

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
    dag.addStream("BlockData", blockReader.messages, blockWriter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput).setLocality(Locality.THREAD_LOCAL);
    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput, tracker.inputFileSplitter);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("MergeTrigger", synchronizer.trigger, merger.input);
    dag.addStream("MergerComplete", merger.output, tracker.inputFileMerger);
  }

  private byte[] getKeyFromConfig(Configuration conf)
  {
    try {
      if (conf.get("dt.application.Ingestion.encrypt.passkey") != null && !conf.get("dt.application.Ingestion.encrypt.passkey").isEmpty()) {
        return conf.get("dt.application.Ingestion.encrypt.passkey").getBytes();
      } else if (conf.get("dt.application.Ingestion.encrypt.keyFile") != null && !conf.get("dt.application.Ingestion.encrypt.keyFile").isEmpty()) {
        return readKeyFromFile(conf.get("dt.application.Ingestion.encrypt.keyFile"));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error initializing encryption key");
    }
    return null;
  }

  private byte[] readKeyFromFile(String keyFileName) throws IOException, EOFException
  {
    File keyFile = new File(keyFileName);
    FileInputStream fis = new FileInputStream(keyFile);
    try {
      return IOUtils.toByteArray(fis);
    } finally {
      fis.close();
    }
  }

  /**
   * DAG for Kafka input source
   * 
   * @param dag
   * @param conf
   */
  private void populateKafkaDAG(DAG dag, Configuration conf, CryptoInformation cryptoInfo)
  {
    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "main_group");

    @SuppressWarnings("resource")
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();

    KafkaSinglePortByteArrayInputOperator inputOpr = dag.addOperator("MessageReader", new KafkaSinglePortByteArrayInputOperator());
    inputOpr.setConsumer(consumer);

    BytesFileOutputOperator outputOpr = dag.addOperator("FileWriter", new BytesFileOutputOperator());
    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if (cryptoInfo != null) {
      CipherStreamProvider cipherProvider = new CipherStreamProvider(cryptoInfo.getTransformation(), cryptoInfo.getSecretKey());
      chainStreamProvider.addStreamProvider(cipherProvider);
    }
    if ("true".equals(conf.get("dt.application.Ingestion.compress"))) {
      chainStreamProvider.addStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());
    }
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

    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if (cryptoInfo != null) {
      CipherStreamProvider cipherProvider = new CipherStreamProvider(cryptoInfo.getTransformation(), cryptoInfo.getSecretKey());
      chainStreamProvider.addStreamProvider(cipherProvider);
    }
    if ("true".equals(conf.get("dt.application.Ingestion.compress"))) {
      chainStreamProvider.addStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());
    }
    if (chainStreamProvider.getStreamProviders().size() > 0) {
      outputOpr.setFilterStreamProvider(chainStreamProvider);
    }

    // Stream connecting reader and writer
    dag.addStream("MessageData", inputOpr.output, outputOpr.input);
  }

  static class CipherStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<CipherOutputStream, OutputStream>
  {
    private Key secretKey;
    private String transformation;

    public CipherStreamProvider(String transformation, Key key)
    {
      this.transformation = transformation;
      secretKey = key;
    }

    @Override
    protected FilterStreamContext<CipherOutputStream> createFilterStreamContext(OutputStream outputStream) throws IOException
    {
      CipherProvider cryptoProvider = new CipherProvider(transformation);
      Cipher cipher = cryptoProvider.getEncryptionCipher(secretKey);
      return new FilterStreamCodec.CipherFilterStreamContext(outputStream, cipher);
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
