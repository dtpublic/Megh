/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.bucket;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * A {@link BucketManager} POJO Implementation that creates buckets based on the Dedup key<br/>
 *
 * @displayName: BasicBucketManagerPOJOImplemenation
 *
 * @since 2.1.0
 */
public class BasicBucketManagerPOJOImpl extends AbstractBucketManager<Object> implements POJOBucketManager<Object>
{
  @NotNull
  private String keyExpression;
  private transient Getter<Object, Object> getter;

  @Override
  public void activate(Context context)
  {
    getter = PojoUtils.createGetter(getPojoClass(), keyExpression, Object.class);
    super.activate(context);
  }

  /**
   * {@inheritDoc} Returns the bucket key for this event.
   */
  @Override
  public long getBucketKeyFor(Object event)
  {
    return Math.abs(((Object)getter.get(event)).hashCode()) % noOfBuckets;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected BucketPOJOImpl createBucket(long bucketKey)
  {
    return new BucketPOJOImpl(bucketKey, keyExpression);
  }

  /**
   * A Java expression that will yield the deduper key from the POJO.
   *
   * @return
   */
  public String getKeyExpression()
  {
    return keyExpression;
  }

  /**
   * Sets the Java expression which will fetch the deduper key from the POJO.
   *
   * @param keyExpression
   */
  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  @Deprecated
  @Override
  public BucketManager<Object> cloneWithProperties()
  {
    throw new UnsupportedOperationException();
  }
}
