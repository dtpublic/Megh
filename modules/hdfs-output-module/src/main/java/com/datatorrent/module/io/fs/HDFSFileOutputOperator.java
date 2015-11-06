package com.datatorrent.module.io.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class HDFSFileOutputOperator<T> extends AbstractFileOutputOperator<T>
{

  String fileName;
  
  @Override
  protected String getFileName(Object tuple)
  {
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(Object tuple)
  { 
    return tuple.toString().getBytes();
  }
  
  public String getFileName()
  {
    return fileName;
  }

  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }
  
  private static Logger LOG = LoggerFactory.getLogger(HDFSOutputModuleDemo.class);
}
