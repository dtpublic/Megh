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
package com.datatorrent.lib.dedup;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * @since 3.4.0
 */
public class DeduperStreamCodec extends KryoSerializableStreamCodec<Object>
{
  private static final long serialVersionUID = 4836484558663411470L;

  private transient Getter<Object, Object> getter;
  private String keyExpression;

  public DeduperStreamCodec(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  @Override
  public int getPartition(Object t)
  {
    if (getter == null) {
      getter = PojoUtils.createGetter(t.getClass(), keyExpression, Object.class);
    }
    return getter.get(t).hashCode();
  }
}
