/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht.wal;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.netlet.util.Slice;

/**
 * Write Log entries to HDFS compatible file systems.
 *
 * @since 3.3.0
 *
 * @param <T>
 */
public class FSWALWriter<T> implements WALWriter<T>
{
  transient DataOutputStream out;
  long committedOffset;
  long unflushed;
  long bucketKey;
  String name;
  LogSerializer<T> serializer;

  public FSWALWriter(FileAccess bfs, LogSerializer<T> serializer, long bucketKey, String name) throws IOException
  {
    this.bucketKey = bucketKey;
    this.name = name;
    out = bfs.getOutputStream(bucketKey, name);
    unflushed = 0;
    committedOffset = 0;
    this.serializer = serializer;
  }

  @Override
  public void close() throws IOException
  {
    if (out != null) {
      out.flush();
      out.close();
    }
  }

  @Override
  public int append(T entry) throws IOException
  {
    Slice slice = serializer.fromObject(entry);
    out.writeInt(slice.length);
    out.write(slice.buffer, slice.offset, slice.length);
    return slice.length + 4;
  }

  @Override
  public void append(byte[] byteBuffer, int length) throws IOException
  {
    out.write(byteBuffer, 0, length);
  }

  @Override
  public void flush() throws IOException
  {
    out.flush();
    if (out instanceof FSDataOutputStream) {
      ((FSDataOutputStream)out).hflush();
      ((FSDataOutputStream)out).hsync();
    }
    committedOffset = out.size();
    unflushed = 0;
  }

  @Override
  public long getSize()
  {
    return out.size();
  }

  @Override
  public String toString()
  {
    return "HDFSWalWritter Bucket " + bucketKey + " fileId " + name;
  }
}
