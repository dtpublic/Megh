/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.module.io.fs;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.io.output.IngestionFileMerger;
import com.datatorrent.module.FSOutputModule;


public class HDFSFileCopyModule extends FSOutputModule
{
  @NotNull
  protected String hostName;
  @NotNull
  protected int port;

  @Override
  public IngestionFileMerger getFileMerger()
  {
    return new HDFSFileMerger();
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

  @Override
  protected String constructFilePath()
  {
    StringBuffer sb = new StringBuffer("hdfs://");
    sb.append(hostName);
    sb.append(":");
    sb.append(port);
    sb.append(directory);
    return sb.toString();
  }
}
