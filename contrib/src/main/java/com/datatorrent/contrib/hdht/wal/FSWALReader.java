/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht.wal;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import com.google.common.base.Preconditions;

import com.datatorrent.contrib.hdht.HDHTFileAccess;
import com.datatorrent.netlet.util.Slice;

/**
 * Read entries from Write Ahead Log during recovery. This implementation is for
 * Log stored in HDFS compatible file system.
 *
 * @param <T>
 */
public class FSWALReader<T> implements WALReader<T>
{
  DataInputStream in;
  T entry = null;
  String name;
  LogSerializer<T> serializer;
  private boolean eof = false;

  public FSWALReader(HDHTFileAccess bfs, LogSerializer<T> serializer, long bucketKey, String name) throws IOException
  {
    this.name = name;
    in = bfs.getInputStream(bucketKey, name);
    this.serializer = serializer;
  }

  @Override
  public void close() throws IOException
  {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public void seek(long offset) throws IOException
  {
    in.skipBytes((int)offset);
  }

  @Override
  public boolean advance() throws IOException
  {
    if (eof) {
      return false;
    }

    try {
      int len = in.readInt();
      Preconditions.checkState(len > 0);

      byte[] data = new byte[len];
      in.readFully(data);

      Slice slice = new Slice(data);
      entry = serializer.toObject(slice);
      return true;
    } catch (EOFException ex) {
      eof = true;
      entry = null;
      return false;
    }
  }

  @Override
  public T get()
  {
    return entry;
  }

}
