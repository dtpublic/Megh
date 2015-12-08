/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.demos.laggards.parser;

import java.io.Serializable;

public class TimeInfo implements Serializable
{
  private static final long serialVersionUID = 201507131412L;

  public long time = 0;

  public TimeInfo()
  {
  }

  public TimeInfo(long time)
  {
    this.time = time;
  }

  public long getTime()
  {
    return time;
  }

  public void setTime(long time)
  {
    this.time = time;
  }
}
