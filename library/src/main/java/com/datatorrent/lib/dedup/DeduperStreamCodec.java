/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class DeduperStreamCodec extends KryoSerializableStreamCodec<Object>
{
  private static final long serialVersionUID = 4836484558663411470L;

  private transient Getter<Object, Object> getter;
  private String keyExpression;
  private Class<?> clazz;

  public DeduperStreamCodec(Class<?> clazz, String keyExpression)
  {
    this.keyExpression = keyExpression;
    this.clazz = clazz;
  }

  @Override
  public int getPartition(Object t)
  {
    if (getter == null) {
      getter = PojoUtils.createGetter(clazz, keyExpression, Object.class);
    }
    return getter.get(t).hashCode();
  }
}
