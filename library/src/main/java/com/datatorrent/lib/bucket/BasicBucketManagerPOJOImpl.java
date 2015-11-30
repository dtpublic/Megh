/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * A {@link BucketManager} that creates buckets based on hash code of the key. This manages all tuples without expiring
 * any tuple.
 */
public class BasicBucketManagerPOJOImpl extends AbstractBucketManagerOptimized<Object> implements
    POJOBucketManager<Object>
{
  @NotNull
  private String keyExpression;
  private transient Getter<Object, Object> getter;

  /**
   * {@inheritDoc} Returns the bucket key for this event.
   */
  @Override
  public long getBucketKeyFor(Object event)
  {
    if (getter == null) {
      Class<?> fqcn = event.getClass();
      Getter<Object, Object> getterTime = PojoUtils.createGetter(fqcn, keyExpression, Object.class);
      getter = getterTime;
    }
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
    return null;
  }

  private static final transient Logger logger = LoggerFactory.getLogger(BasicBucketManagerPOJOImpl.class);
}
