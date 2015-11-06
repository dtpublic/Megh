package com.datatorrent.module.io.fs;

import java.io.Serializable;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.partitioner.StatelessPartitioner;

/**
 * HDFSOutputModule is responsible for writing tuples from the stream onto
 * HDFS. It assumes that all tuples from the stream are to be written to same file
 * on HDFS. Additionally, it supports writing to files in rolling mode. In this
 * case, output will be rolled over to the next file (with auto-increment
 * suffix) when <code>maxLength</code> bytes are written to the file.
 * 
 * @author Yogi Devendra
 * 
 * @param <T>
 */

public class HDFSOutputModule<T extends Serializable> implements Module
{

  @NotNull
  @Size(min = 1)
  private String hostName;
  @NotNull
  @Min(0)
  private int port;
  @NotNull
  @Size(min = 1)
  private String directory;
  @NotNull
  @Size(min = 1)
  private String fileName;
  
  private String tupleSeparator = System.getProperty("line.separator");

  public static final String HDFS_SCHEME = "hdfs";

  //The maximum length in bytes of a rolling file.
  @Min(0)
  protected Long maxLength = Long.MAX_VALUE;

  //No. of static partitions to be used for this module
  @Min(1)
  protected int partitionCount = 1;

  public transient ProxyInputPort<T> input = new Module.ProxyInputPort<T>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Defining DAG
    HDFSFileOutputOperator<T> hdfsFileOutputOperator = dag.addOperator("HDFSOutputOperator",
        new HDFSFileOutputOperator<T>());

    //Setting properties
    hdfsFileOutputOperator.setFilePath(constructFilePath());
    hdfsFileOutputOperator.setFileName(fileName);
    hdfsFileOutputOperator.setMaxLength(maxLength);
    hdfsFileOutputOperator.setTupleSeparator(tupleSeparator);
    dag.setAttribute(hdfsFileOutputOperator, Context.OperatorContext.PARTITIONER,
        new StatelessPartitioner<HDFSFileOutputOperator<T>>(partitionCount));

    //Binding proxy ports 
    input.set(hdfsFileOutputOperator.input);

  }

  /**
   * @return Host name of the HDFS name node service
   */
  public String getHostName()
  {
    return hostName;
  }

  /**
   * Fully qualified domain name or IP for HDFS name node service.
   * 
   * @param hostName
   */
  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }

  /**
   * 
   * @return Port number of the HDFS service
   */
  public int getPort()
  {
    return port;
  }

  /**
   * Port number for HDFS file system
   * 
   * @param port
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * @return Location of the HDFS file
   */
  public String getDirectory()
  {
    return directory;
  }

  /**
   * Location of the file to write the tuples
   * 
   * @param directory
   */
  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  /**
   * @return Name of the file
   */
  public String getFileName()
  {
    return fileName;
  }

  /**
   * Name of the file to write data
   * 
   * @param fileName
   */
  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }

  /**
   * @return Maximum length of the output file after which file will be rolled
   *         over to the next file
   */
  public Long getMaxLength()
  {
    return maxLength;
  }

  /**
   * Max file size decides number of bytes after which file should roll over. If
   * no value is specified then files will not be rolled over.
   * 
   * @param maxLength
   */
  public void setMaxLength(Long maxLength)
  {
    this.maxLength = maxLength;
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * No. of static partitions
   * 
   * @param partitionCount
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }
  
  /**
   * @return separator Separator between the tuples
   */
  public String getTupleSeparator()
  {
    return tupleSeparator;
  }
  
  /**
   * @param separator Separator between the tuples
   * Default value is newline if separator is not set explicitly. 
   */
  
  public void setTupleSeparator(String tupleSeparator)
  {
    this.tupleSeparator = tupleSeparator;
  }

  /**
   * Constructs filePath from the specified configuration
   * 
   * @return
   */
  private String constructFilePath()
  {
    return new StringBuffer(HDFS_SCHEME).append("://").append(hostName).append(":").append(port).append(directory)
        .toString();
  }

  private static Logger LOG = LoggerFactory.getLogger(HDFSOutputModule.class);

}
