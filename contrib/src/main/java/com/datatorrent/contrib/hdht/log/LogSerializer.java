/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht.log;

import com.datatorrent.netlet.util.Slice;

public interface LogSerializer<T>
{
  public Slice fromObject(T entry);

  public T toObject(Slice s);
}
