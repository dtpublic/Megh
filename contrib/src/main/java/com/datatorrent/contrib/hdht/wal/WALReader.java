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

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @since 3.3.0
 */

public interface WALReader<T> extends Closeable
{
  /**
   * Close WAL after read.
   *
   * @throws IOException
   */
  @Override
  void close() throws IOException;

   /**
   * Seek to middle of the WAL. This is used primarily during recovery,
   * when we need to start recovering data from middle of WAL file.
   */
  void seek(long offset) throws IOException;

  /**
   * Advance WAL by one entry, returns true if it can advance, else false
   * in case of any other error throws an Exception.
   *
   * @return true if next data item is read successfully, false if data can not be read.
   * @throws IOException
   */
  boolean advance() throws IOException;

  /**
   * Return current entry from WAL, returns null if end of file has reached.
   *
   * @return MutableKeyValue
   */
  T get();
}
