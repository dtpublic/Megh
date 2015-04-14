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
import com.datatorrent.apps.ingestion.io.output.FileMerger;
import com.datatorrent.apps.ingestion.io.s3.S3BlockReader;
import com.datatorrent.apps.ingestion.lib.AESCryptoProvider;
import com.datatorrent.apps.ingestion.lib.SymmetricKeyManager;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.fs.FilterStreamCodec;
import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.lib.io.fs.FilterStreamProvider;

@ApplicationAnnotation(name = "Ingestion")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    IngestionFileSplitter fileSplitter = dag.addOperator("FileSplitter", new IngestionFileSplitter());
    dag.setAttribute(fileSplitter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockReader blockReader;
    if (Application.Schemes.FTP.equals(conf.get("dt.operator.BlockReader.prop.scheme"))) {
      blockReader = dag.addOperator("BlockReader", new FTPBlockReader());
    } else if (Application.Schemes.S3N.equals(conf.get("dt.operator.BlockReader.prop.scheme"))) {
      blockReader = dag.addOperator("BlockReader", new S3BlockReader());
    } else {
      blockReader = dag.addOperator("BlockReader", new BlockReader());
    }
    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    dag.setAttribute(blockWriter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    FileMerger merger = dag.addOperator("FileMerger", new FileMerger());
    // ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();

    if ("true".equals(conf.get("dt.application.Ingestion.compress"))) {
      chainStreamProvider.addStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());
    }
    if ("true".equals(conf.get("dt.application.Ingestion.encrypt"))) {
      CipherStreamProvider cipherProvider = initializeCipherProvider(conf.get("dt.application.Ingestion.secretKeyFile"));
      chainStreamProvider.addStreamProvider(cipherProvider);
      merger.setEncrypt(true);
      merger.setSecret(cipherProvider.secret);
    }
    if (chainStreamProvider.getStreamProviders().size() > 0) {
      blockWriter.setFilterStreamProvider(chainStreamProvider);
    }

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
    dag.addStream("BlockData", blockReader.messages, blockWriter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput).setLocality(Locality.THREAD_LOCAL);
    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("MergeTrigger", synchronizer.trigger, /* console.input, */merger.input);
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

  public static interface Schemes
  {
    String FILE = "file";
    String FTP = "ftp";
    String S3N = "s3n";
    String HDFS = "hdfs";
  }

}
