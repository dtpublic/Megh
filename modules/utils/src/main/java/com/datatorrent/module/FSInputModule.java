/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.metric.MetricsAggregator;
import com.datatorrent.common.metric.SingleMetricAggregator;
import com.datatorrent.common.metric.sum.LongSumAggregator;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.input.AbstractFileSplitter;
import com.datatorrent.lib.io.input.BlockReader;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.netlet.util.Slice;

/**
 * FSInputModule is an abstract module for file system input modules like HDFS, S3, NFS, etc.
 * It read files and emits FileMetadata, BlockMetadata and the block bytes.&nbsp;
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No proxy input port<br>
 * <b>Output</b>: 3 output ports for FileMetadata, BlockMetadata and block bytes<br>
 * <br>
 * Properties:<br>
 * <b>files</b>: Comma separated list of files/directories to scan. <br>
 * <b>filePatternRegularExp</b>: Files with names matching the given java regular expression are split. <br>
 * <b>bandwidth</b>: Specifies the bandwidth limit. <br>
 * <b>scanIntervalMillis</b>: Specifies the scan interval in milliseconds, interval between two scans to discover
 *                            new files in input directory. <br>
 * <b>recursive</b>: Specifies the scan be recursive or not. <br>
 * <b>blockSize</b>: Specifies the block size to read input blocks of file. <br>
 * <b>sequencialFileRead</b>: Specifies the file read be sequential or not.
 * <b>readersCount</b>: Specifies the number of parallel readers to read blocks.
 * <br>
 * <\p>
 */
public abstract class FSInputModule implements Module
{
  @NotNull
  @Size(min = 1)
  private String files;
  private String filePatternRegularExp;
  private long bandwidth;
  @Min(0)
  private long scanIntervalMillis;
  private boolean recursive = true;
  private long blockSize;
  private boolean sequencialFileRead = false;
  private int readersCount;

  /**
   * Creates the file splitter instance
   * @return the created file splitter instance
   */
  public abstract ModuleFileSplitter getFileSplitter();

  /**
   * Creates the BlockReader instance
   * @return the block reader
   */
  public abstract BlockReader getBlockReader();

  public final transient ProxyOutputPort<AbstractFileSplitter.FileMetadata> filesMetadataOutput = new ProxyOutputPort();
  public final transient ProxyOutputPort<BlockMetadata.FileBlockMetadata> blocksMetadataOutput = new ProxyOutputPort();
  public final transient ProxyOutputPort<AbstractBlockReader.ReaderRecord<Slice>> messages = new ProxyOutputPort();

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    ModuleFileSplitter fileSplitter = dag.addOperator("FileSplitter", getFileSplitter());
    BlockReader blockReader = dag.addOperator("BlockReader", getBlockReader());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);

    // Sets the proxy ports
    filesMetadataOutput.set(fileSplitter.filesMetadataOutput);
    blocksMetadataOutput.set(blockReader.blocksMetadataOutput);
    messages.set(blockReader.messages);

    // Set the module properties to operators
    if (blockSize != 0) {
      fileSplitter.setBlockSize(blockSize);
    }
    fileSplitter.setSequencialFileRead(sequencialFileRead);
    ModuleFileSplitter.Scanner fileScanner = (ModuleFileSplitter.Scanner)fileSplitter.getScanner();
    fileScanner.setFiles(files);
    if (scanIntervalMillis != 0) {
      fileScanner.setScanIntervalMillis(scanIntervalMillis);
    }
    fileScanner.setRecursive(recursive);
    if (filePatternRegularExp != null) {
      fileSplitter.getScanner().setFilePatternRegularExp(filePatternRegularExp);
    }
    if (bandwidth != 0) {
      fileSplitter.getBandwidthManager().setBandwidth(bandwidth);
    }

    if (readersCount != 0) {
      dag.setAttribute(blockReader, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<>(
          readersCount));
    }

    MetricsAggregator blockReaderMetrics = new MetricsAggregator();
    blockReaderMetrics.addAggregators("bytesReadPerSec", new SingleMetricAggregator[] {new LongSumAggregator() });
    dag.setAttribute(blockReader, Context.OperatorContext.METRICS_AGGREGATOR, blockReaderMetrics);
    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());
  }

  /**
   * Gets the files to be scanned.
   *
   * @return files to be scanned.
   */
  public String getFiles()
  {
    return files;
  }

  /**
   * A comma separated list of files/directories to scan.
   *
   * @param files files
   */
  public void setFiles(String files)
  {
    this.files = files;
  }

  /**
   * Gets the regular expression for file names to split
   *
   * @return regular expression
   */
  public String getFilePatternRegularExp()
  {
    return filePatternRegularExp;
  }

  /**
   * Only files with names matching the given java regular expression are split
   *
   * @param filePatternRegularExp regular expression
   */
  public void setFilePatternRegularExp(String filePatternRegularExp)
  {
    this.filePatternRegularExp = filePatternRegularExp;
  }

  /**
   * Gets bandwidth limit value.
   *
   * @return bandwidth limit value
   */
  public long getBandwidth()
  {
    return bandwidth;
  }

  /**
   * Sets bandwidht limit value
   *
   * @param bandwidth
   */
  public void setBandwidth(long bandwidth)
  {
    this.bandwidth = bandwidth;
  }

  /**
   * Gets scan interval in milliseconds, interval between two scans to discover
   * new files in input directory
   *
   * @return scanInterval milliseconds
   */
  public long getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  /**
   * Sets scan interval in milliseconds, interval between two scans to discover
   * new files in input directory
   *
   * @param scanIntervalMillis
   */
  public void setScanIntervalMillis(long scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  /**
   * Get is scan recursive
   *
   * @return isRecursive
   */
  public boolean isRecursive()
  {
    return recursive;
  }

  /**
   * set is scan recursive
   *
   * @param recursive
   */
  public void setRecursive(boolean recursive)
  {
    this.recursive = recursive;
  }

  /**
   * Get block size used to read input blocks of file
   *
   * @return blockSize
   */
  public long getBlockSize()
  {
    return blockSize;
  }

  /**
   * Sets block size used to read input blocks of file
   *
   * @param blockSize
   */
  public void setBlockSize(long blockSize)
  {
    this.blockSize = blockSize;
  }

  /**
   * Gets is sequencial file read
   *
   * @return
   */
  public boolean isSequencialFileRead()
  {
    return sequencialFileRead;
  }

  /**
   * Sets is sequencial file read
   *
   * @param sequencialFileRead
   */
  public void setSequencialFileRead(boolean sequencialFileRead)
  {
    this.sequencialFileRead = sequencialFileRead;
  }

  /**
   * Return the number of parallel readers
   * @return the readersCount
   */
  public int getReadersCount()
  {
    return readersCount;
  }

  /**
   * Sets the number of parallel readers
   * @param readersCount
   */
  public void setReadersCount(int readersCount)
  {
    this.readersCount = readersCount;
  }
}
