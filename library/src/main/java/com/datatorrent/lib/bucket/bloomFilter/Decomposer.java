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
package com.datatorrent.lib.bucket.bloomFilter;

import java.nio.charset.Charset;

/**
 * Contract for implementations that wish to help in decomposing an object to a
 * byte-array so that various hashes can be computed over the same.
 *
 * @since 3.3.0
 *
 * @param <T>
 *          the type of object over which this decomposer works
 */
public interface Decomposer<T>
{

  /**
   * Decompose the object into the byte-array
   *
   * @param object
   *          the object to be decomposed
   */
  byte[] decompose(T object);

  public class DefaultDecomposer<T> implements Decomposer<T>
  {
    /**
     * The default platform encoding
     */
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    /**
     * Decompose the object
     */
    @Override
    public byte[] decompose(T object)
    {
      if (object == null) {
        return null;
      }
      if (object instanceof String) {
        return (((String)object).getBytes(DEFAULT_CHARSET));
      }
      return (object.toString().getBytes(DEFAULT_CHARSET));
    }

  }
}
