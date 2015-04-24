/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

/**
 * @author Yogi/Sandeep
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;

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
import com.datatorrent.apps.ingestion.io.jms.BytesFileOutputOperator;
import com.datatorrent.apps.ingestion.io.jms.JMSBytesInputOperator;
import com.datatorrent.apps.ingestion.io.output.FileMerger;
import com.datatorrent.apps.ingestion.io.output.HDFSFileMerger;
import com.datatorrent.apps.ingestion.io.s3.S3BlockReader;
import com.datatorrent.apps.ingestion.kafka.FileOutputOperator;
import com.datatorrent.apps.ingestion.lib.AESCryptoProvider;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.FilterStreamCodec;
import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.lib.io.fs.FilterStreamProvider;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

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
      populateKafkaDAG(dag, conf);
      break;
    case JMS:
      populateJMSDAG(dag, conf);
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
    
    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();

    if (conf.getBoolean("dt.application.Ingestion.compress", false)) {
      chainStreamProvider.addStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());
    }
    if (conf.getBoolean("dt.application.Ingestion.encrypt", false)) {
      CipherStreamProvider cipherProvider = initializeCipherProvider(conf.get("dt.application.Ingestion.secretKeyFile"));
      chainStreamProvider.addStreamProvider(cipherProvider);
      merger.setEncrypt(true);
      merger.setSecret(cipherProvider.secret);
    }
    if (chainStreamProvider.getStreamProviders().size() > 0) {
      blockWriter.setFilterStreamProvider(chainStreamProvider);
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

  private CipherStreamProvider initializeCipherProvider(String keyFileName)
  {
    CipherStreamProvider cipherProvider;
    if (keyFileName != null && !keyFileName.isEmpty()) {
      try {
        byte[] key = readKeyFromFile(keyFileName);
        cipherProvider = new CipherStreamProvider(key);
      } catch (IOException e) {
        throw new RuntimeException("Error initializing key from keyFile: " + keyFileName, e);
      }
    } else {
      cipherProvider = new CipherStreamProvider();
    }
    return cipherProvider;
  }

  private byte[] readKeyFromFile(String keyFileName) throws IOException
  {
    File keyFile = new File(keyFileName);
    FileInputStream fis = new FileInputStream(keyFile);
    try {
      byte[] keyBytes = new byte[32];
      int keySize = fis.read(keyBytes);
      return Arrays.copyOf(keyBytes, keySize);
    } finally {
      fis.close();
    }
  }

  static class CipherStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<CipherOutputStream, OutputStream>
  {
    @Bind(JavaSerializer.class)
    private SecretKey secret;

    public CipherStreamProvider()
    {
      secret = SymmetricKeyManager.getInstance().generateSymmetricKeyForAES();
    }

    public CipherStreamProvider(byte[] key)
    {
      secret = SymmetricKeyManager.getInstance().generateSymmetricKeyForAES(key);
    }

    @Override
    protected FilterStreamContext<CipherOutputStream> createFilterStreamContext(OutputStream outputStream) throws IOException
    {
      AESCryptoProvider cryptoProvider = new AESCryptoProvider();
      Cipher cipher = cryptoProvider.getEncryptionCipher(secret);
      return new FilterStreamCodec.CipherFilterStreamContext(outputStream, cipher);
    }
  }

  /**
   * DAG for Kafka input source
   * 
   * @param dag
   * @param conf
   */
  private void populateKafkaDAG(DAG dag, Configuration conf)
  {
    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "main_group");

    @SuppressWarnings("resource")
    HighlevelKafkaConsumer consumer = new HighlevelKafkaConsumer(props);

    KafkaSinglePortStringInputOperator inputOpr = dag.addOperator("MessageReader", new KafkaSinglePortStringInputOperator());
    inputOpr.setConsumer(consumer);

    FileOutputOperator outputOpr = dag.addOperator("FileWriter", new FileOutputOperator());
    dag.addStream("kafkaData", inputOpr.outputPort, outputOpr.input);
  }

  /**
   * DAG for JMS input source
   * 
   * @param dag
   * @param conf
   */
  public void populateJMSDAG(DAG dag, Configuration conf)
  {
    // Reads from JMS
    JMSBytesInputOperator inputOpr = dag.addOperator("MessageReader", new JMSBytesInputOperator());
    // Writes to file
    BytesFileOutputOperator outputOpr = dag.addOperator("FileWriter", new BytesFileOutputOperator());
    // Stream connecting reader and writer
    dag.addStream("JMSData", inputOpr.output, outputOpr.input);
  }

  public static enum Scheme {
    FILE {
      @Override
      public String toString()
      {
        return "file";
      }
    },
    FTP {
      @Override
      public String toString()
      {
        return "ftp";
      }
    },
    S3N {
      @Override
      public String toString()
      {
        return "s3n";
      }
    },
    HDFS {
      @Override
      public String toString()
      {
        return "hdfs";
      }
    },
    KAFKA {
      @Override
      public String toString()
      {
        return "kafka";
      }
    },
    JMS {
      @Override
      public String toString()
      {
        return "jms";
      }
    }
  }

}
