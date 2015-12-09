/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the specialized implementation of a Ordered BucketManager for Time based expiry. This implementation uses an
 * Expirable bucket store. Sub-classes must implement the getTime method which gets the time field of the incoming
 * tuple. The expiry key (time field) is assumed to be an ordered numeric field (time in seconds) based on which we can
 * create buckets and expire incoming tuples.
 *
 * @param <T>
 */
public abstract class AbstractTimeBasedBucketManager<T> extends AbstractOrderedBucketManager<T>
{
  // Default
  public static long DEF_MAX_TIME_JUMP = 60 * 60 * 3; // 2 hours
  public static long DEF_EXPIRY_PERIOD = 60 * 60 * 24; // 1 day
  public static long DEF_BUCKET_SPAN = 60; // 1 min

  /**
   * Signifies whether the System time or the tuple time is used to advance the expiry point.
   */
  private boolean useSystemTime = true;

  public AbstractTimeBasedBucketManager()
  {
    super();
    bucketSpan = DEF_BUCKET_SPAN;
    expiryPeriod = DEF_EXPIRY_PERIOD;
    maxExpiryJump = DEF_MAX_TIME_JUMP;
  }

  /**
   * Returns the value of the time field of the tuple. Assumes that the key returned will be the time field in
   * milliseconds.
   *
   * @param event
   * @return value of time field in event in milliseconds
   */
  protected abstract long getTime(T event);

  @Override
  protected void recomputeNumBuckets()
  {
    if (useSystemTime) {
      // System time
      Calendar calendar = Calendar.getInstance();
      long now = calendar.getTimeInMillis() / 1000;
      calendar.add(Calendar.SECOND, (int)-expiryPeriod);
      startOfBuckets = calendar.getTimeInMillis() / 1000;
      expiryPoint = startOfBuckets;
      noOfBuckets = (int)Math.ceil((now - startOfBuckets) / (bucketSpan * 1.0)) + 1;
      logger.debug("Now {}, StartOfBuckets {}, Expiry Point {}, Num Buckets {}, Expiry Period: {}", now,
          startOfBuckets, expiryPoint, noOfBuckets, expiryPeriod);
    } else {
      // Tuple time
      startOfBuckets = 0;
      expiryPoint = startOfBuckets;
      noOfBuckets = (int)Math.ceil((expiryPeriod) / (bucketSpan * 1.0)) + 1;
    }

    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxExpiryPerBucket = new long[noOfBuckets];
  }

  @Override
  protected void processExpiry(long expiryKey)
  {
    // If using System time to advance expiry point, then do it
    if (useSystemTime) {
      endOfBuckets = System.currentTimeMillis() / 1000;
      expiryPoint = endOfBuckets - expiryPeriod + 1;
    } else {
      super.processExpiry(expiryKey);
    }
  }

  /**
   * Returns the expiry key based on which the buckets will be formed and incoming tuples will be expired. Expiry key is
   * expected to be in seconds.
   *
   * @param event
   * @return long expiry key - in seconds
   */
  @Override
  protected long getExpiryKey(T event)
  {
    return getTime(event) / 1000; // Expect time in millis from getTime(). Convert to seconds.
  }

  /**
   * Check whether to use System time to advance expiry point. If set to false, Tuple time will be used to advance the
   * expiry point, else System time will be used.
   *
   * @return boolean use system time
   */
  public boolean isUseSystemTime()
  {
    return useSystemTime;
  }

  /**
   * Sets whether to use System time to advance expiry point. If set to false, Tuple time will be used to advance the
   * expiry point, else System time will be used.
   *
   * @param useSystemTime
   */
  public void setUseSystemTime(boolean useSystemTime)
  {
    this.useSystemTime = useSystemTime;
  }

  private static final transient Logger logger = LoggerFactory.getLogger(AbstractTimeBasedBucketManager.class);
}
