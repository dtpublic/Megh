/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app.aggregation.customdata;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class Output extends AbstractFileOutputOperator<String>
{
  private String outputFileName;

  @Override
  protected String getFileName(String s)
  {
    return outputFileName;
  }

  @Override
  protected byte[] getBytesForTuple(String s)
  {
    return (s + "\n").getBytes();
  }

  public String getOutputFileName()
  {
    return outputFileName;
  }

  public void setOutputFileName(String outputFileName)
  {
    this.outputFileName = outputFileName;
  }
}
