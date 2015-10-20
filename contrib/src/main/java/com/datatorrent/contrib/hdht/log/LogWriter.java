/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.hdht.log;

import java.io.IOException;

/**
 * WAL writer interface.
 */
public interface LogWriter<T> extends Cloneable
{
  /**
   * flush pending data to disk and close file.
   *
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   */
  public int append(T entry) throws IOException;

  /**
   * Flush data to persistent storage.
   *
   * @throws IOException
   */
  public void flush() throws IOException;

  /**
   * Return count of byte which may not be flushed to persistent storage.
   *
   * @return The count of byte which may not be flushed to persistent storage.
   */
  public long getUnflushedCount();

  /**
   * Returns offset of the file, till which data is known to be persisted on the disk.
   *
   * @return The offset of the file, till which data is known to be persisted on the disk.
   */
  public long getCommittedLen();

  /**
   * Returns file size, last part of the file may not be persisted on disk.
   *
   * @return The file size, last part of the file may not be persisted on disk.
   */
  public long logSize();
}
