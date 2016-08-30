/**
 * Copyright (c) 2016 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
