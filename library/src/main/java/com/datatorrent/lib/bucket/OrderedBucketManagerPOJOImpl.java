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
 * A {@link BucketManager} implementation which creates buckets based on an ordered key. This implementation takes a
 * POJO as an input.
 *
 * @displayName: OrderedBucketManagerPOJOImplemenation
 */
public class OrderedBucketManagerPOJOImpl extends AbstractOrderedBucketManager<Object>
    implements POJOBucketManager<Object>
{
  @NotNull
  private String expiryExpression;
  @NotNull
  private String keyExpression;
  private transient Getter<Object, Object> getter;

  /**
   * Returns the expiry key of the tuple. The expiry key is assumed to be a long value in the domain of the expiry key.
   */
  @Override
  protected long getExpiryKey(Object event)
  {
    if (getter == null) {
      Class<?> fqcn = event.getClass();
      Getter<Object, Object> getterObj = PojoUtils.createGetter(fqcn, expiryExpression, Object.class);
      getter = getterObj;
    }
    return Long.parseLong(getter.get(event).toString());
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
   * @return keyExpression
   */
  public String getKeyExpression()
  {
    return keyExpression;
  }

  /**
   * Sets the Java expression for fetching the deduper key field from the POJO
   *
   * @param keyExpression
   */
  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  /**
   * A Java expression that will yield the value of the expiry field from the POJO. This expression needs to evaluate to
   * long
   *
   * @return expiryExpression
   */
  public String getExpiryExpression()
  {
    return expiryExpression;
  }

  /**
   * Sets the Java expression that will yield the value of the expiry field from the POJO. This expression needs to
   * evaluate to long
   *
   * @param expiryExpression
   */
  public void setExpiryExpression(String expiryExpression)
  {
    this.expiryExpression = expiryExpression;
  }

  private static final transient Logger logger = LoggerFactory.getLogger(OrderedBucketManagerPOJOImpl.class);
}
