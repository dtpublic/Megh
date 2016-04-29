/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * A {@link BucketManager} that creates buckets based on a categorical key. This implementation takes a POJO as an
 * input.
 *
 * @displayName: CategoricalBucketManagerPOJOImplemenation
 */
public class CategoricalBucketManagerPOJOImpl extends AbstractCategoricalBucketManager<Object>
    implements POJOBucketManager<Object>
{
  @NotNull
  private String expiryExpression;
  @NotNull
  private String keyExpression;
  private transient Getter<Object, Object> getterExpiry;
  private transient Getter<Object, Object> getterKey;

  @Override
  public void activate(Context context)
  {
    getterKey = PojoUtils.createGetter(getPojoClass(), keyExpression, Object.class);
    getterExpiry = PojoUtils.createGetter(getPojoClass(), expiryExpression, Object.class);
    super.activate(context);
  }

  /**
   * Returns the expiry field from the POJO. The returned value is expected to be a string value since this is assumed
   * to be a categorical key.
   */
  @Override
  protected String getExpiryKey(Object event)
  {
    return getterExpiry.get(event).toString();
  }

  /**
   * Returns the value of the deduper key field from the POJO.
   */
  @Override
  protected Object getEventKey(Object event)
  {
    return getterKey.get(event);
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
   * Sets the Java keyExpression for fetching the deduper key field from the POJO
   *
   * @param keyExpression
   */
  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  /**
   * A Java expression that will yield the value of expiry key from the POJO.
   *
   * @return expiryExpression
   */
  public String getExpiryExpression()
  {
    return expiryExpression;
  }

  /**
   * Sets the Java expression that will yield the value of expiry field from the POJO.
   *
   * @param expiryExpression
   */
  public void setExpiryExpression(String expiryExpression)
  {
    this.expiryExpression = expiryExpression;
  }

  private static final transient Logger logger = LoggerFactory
      .getLogger(CategoricalBucketManagerPOJOImpl.class);
}
