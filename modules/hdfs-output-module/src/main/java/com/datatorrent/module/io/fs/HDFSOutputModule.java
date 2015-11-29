package com.datatorrent.module.io.fs;

import java.io.Serializable;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.partitioner.StatelessPartitioner;

public class HDFSOutputModule<T extends Serializable> implements Module
{
  
  @NotNull
  protected String hostName;
  @NotNull
  protected int port;
  @NotNull
  protected String directory;
  @NotNull
  protected String fileName;
  
  //The maximum length in bytes of a rolling file.
  Long maxLength = Long.MAX_VALUE;

  //No. of static partitions to be used for this module
  int partitionCount=1;
  
  public transient ProxyInputPort<T> input = new Module.ProxyInputPort<T>();
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Defining DAG
    HDFSFileOutputOperator<T> hdfsFileOutputOperator = dag.addOperator("HDFSOutputOperator", new HDFSFileOutputOperator<T>());
    
    //Setting properties
    hdfsFileOutputOperator.setFilePath(constructFilePath());
    hdfsFileOutputOperator.setFileName(fileName);
    hdfsFileOutputOperator.setMaxLength(maxLength);
    dag.setAttribute(hdfsFileOutputOperator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<HDFSFileOutputOperator<T>>(partitionCount));
    
    //Binding proxy ports 
    input.set(hdfsFileOutputOperator.input);
    
  }

  public String getHostName()
  {
    return hostName;
  }
  
  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }
  
  public int getPort()
  {
    return port;
  }
  
  public void setPort(int port)
  {
    this.port = port;
  }
  
  public String getDirectory()
  {
    return directory;
  }
  
  public void setDirectory(String directory)
  {
    this.directory = directory;
  }
  
  public String getFileName()
  {
    return fileName;
  }
  
  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }
  
  
  
  public Long getMaxLength()
  {
    return maxLength;
  }
  
  public void setMaxLength(Long maxLength)
  {
    this.maxLength = maxLength;
  }
  
  private String constructFilePath(){
    StringBuffer sb = new StringBuffer("hdfs://");
    sb.append(hostName);
    sb.append(":");
    sb.append(port);
    sb.append(directory);
    return sb.toString();
  }
  
  private static Logger LOG = LoggerFactory.getLogger(HDFSOutputModule.class);

}
