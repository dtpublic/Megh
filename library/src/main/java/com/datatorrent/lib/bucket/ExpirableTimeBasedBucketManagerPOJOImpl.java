/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import java.util.Date;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * A {@link BucketManager} implementation which creates buckets based on time. This implementation takes a POJO as an
 * input.
 *
 * @displayName: TimeBasedBucketManagerPOJOImplemenation
 */
public class ExpirableTimeBasedBucketManagerPOJOImpl extends AbstractExpirableTimeBasedBucketManager<Object>
{
  @NotNull
  private String timeExpression;
  @NotNull
  private String keyExpression;
  private transient Getter<Object, Date> getter;

  /**
   * Returns the value of the time field from the POJO. The return value is in millis.
   */
  @Override
  protected long getTime(Object event)
  {
    if (getter == null) {
      Class<?> fqcn = event.getClass();
      Getter<Object, Date> getterTime = PojoUtils.createGetter(fqcn, timeExpression, Date.class);
      getter = getterTime;
    }
    return ((Date)getter.get(event)).getTime();
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
   * Sets the Java expression for fetching the deduper key field from the POJO
   *
   * @param keyExpression
   */
  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  /**
   * A Java expression that will yield the timestamp value from the POJO. This expression needs to evaluate to a Java
   * Date type
   *
   * @return timeExpression
   */
  public String getTimeExpression()
  {
    return timeExpression;
  }

  /**
   * Sets the Java expression that will yield the timestamp value from the POJO. This expression needs to evaluate to a
   * Java Date type
   *
   * @param timeExpression
   */
  public void setTimeExpression(String timeExpression)
  {
    this.timeExpression = timeExpression;
  }

  private static final transient Logger logger = LoggerFactory.getLogger(ExpirableTimeBasedBucketManagerPOJOImpl.class);
}
