/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht.wal;

import java.io.IOException;

/**
 * WAL writer interface.
 * @since 3.3.0
 *
 */
public interface WALWriter<T> extends Cloneable
{
  /**
   * flush pending data to disk and close file.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Write an entry to the WAL, this operation need not flush the data.
   */
  int append(T entry) throws IOException;
  
  /**
   * Write specified number of bytes to WAL with at the end.
   * @param byteBuffer
   * @param length
   * @throws IOException
   */
  void append(byte[] byteBuffer, int length) throws IOException;

  /**
   * Flush data to persistent storage.
   *
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Returns size of the WAL, last part of the log may not be persisted on disk.
   * In case of file backed WAL this will be the size of file, in case of kafka
   * like log, this will be similar to the message offset.
   *
   * @return The log size
   */
  long getSize();

}
