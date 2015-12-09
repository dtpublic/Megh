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

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.AutoMetric;


/**
 * This is the base implementation of OrderedBucketManager. Subclasses must implement the {@link #getExpiryKey(Object)}
 * method which returns the expiry key, based on which expiry is done. The expiry key is assumed to be an ordered
 * numeric field based on which we can create buckets and expire incoming tuples.
 *
 * @param <T>
 *          type of the incoming tuple
 *
 * @since 2.1.0
 */
public abstract class AbstractOrderedBucketManager<T> extends AbstractBucketManager<T>
{
  public static long DEF_MAX_EXPIRY_JUMP = Long.MAX_VALUE;
  public static long DEF_EXPIRY_PERIOD = 10000;
  public static long DEF_BUCKET_SPAN = 100;

  @Min(0)
  protected long expiryPeriod = DEF_EXPIRY_PERIOD;
  @Min(1)
  protected long bucketSpan = DEF_BUCKET_SPAN;
  @AutoMetric
  protected long expiryPoint;
  protected long[] maxExpiryPerBucket;
  protected long maxExpiryJump = DEF_MAX_EXPIRY_JUMP;

  /**
   * Sub classes implementing this method will return the expiry key of the incoming tuple. The expiry key is expected
   * to be a long key which can be used for expiry.
   *
   * @param event
   * @return long expiry key of tuple
   */
  protected abstract long getExpiryKey(T event);

  @Deprecated
  @Override
  public AbstractOrderedBucketManager<T> cloneWithProperties()
  {
    return null;
  }

  @Override
  public void setBucketStore(@Nonnull BucketStore<T> store)
  {
    Preconditions.checkArgument(store instanceof BucketStore.ExpirableBucketStore);
    this.bucketStore = store;
    recomputeNumBuckets();
  }

  protected void recomputeNumBuckets()
  {
    startOfBuckets = 0;
    expiryPoint = startOfBuckets;
    noOfBuckets = (int)Math.ceil((expiryPeriod) / (bucketSpan * 1.0)) + 1;

    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxExpiryPerBucket = new long[noOfBuckets];
  }

  @Override
  public void startService(Listener<T> listener)
  {
    endOfBuckets = expiryPoint + expiryPeriod - 1;
    logger.debug("bucket parameters expiry period {}, bucket span {}", expiryPeriod, bucketSpan);
    logger.debug("bucket manager properties expiry {}, end {}", expiryPoint, endOfBuckets);

    super.startService(listener);
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    long expiryKey = getExpiryKey(event);
    if (expiryKey < expiryPoint) {
      return -1;
    }
    if (expiryPoint > 0 && expiryKey - endOfBuckets > maxExpiryJump) {
      return -2;
    }

    // Process the expiry due to this tuple
    processExpiry(expiryKey);

    long key = expiryKey / bucketSpan;
    return key;
  }

  /**
   * Updates the expiry window end points: {@link #expiryPoint} and endOfBuckets
   *
   * @param expiryKey
   */
  protected void processExpiry(long expiryKey)
  {
    // Move expiry point and end of buckets
    if (expiryKey > endOfBuckets) {
      endOfBuckets = expiryKey;
      expiryPoint = endOfBuckets - expiryPeriod + 1;
    }
  }

  @Override
  public void newEvent(long bucketKey, T event)
  {
    super.newEvent(bucketKey, event);

    int bucketIdx = (int)(bucketKey % noOfBuckets);
    Long max = maxExpiryPerBucket[bucketIdx];
    long expiryKey = getExpiryKey(event);
    if (max == 0 || expiryKey > max) {
      maxExpiryPerBucket[bucketIdx] = expiryKey;
    }
  }

  @Override
  public void endWindow(long window)
  {
    long maxTime = 0;
    for (int bucketIdx : dirtyBuckets.keySet()) {
      if (maxExpiryPerBucket[bucketIdx] > maxTime) {
        maxTime = maxExpiryPerBucket[bucketIdx];
      }
      maxExpiryPerBucket[bucketIdx] = 0;
    }
    if (maxTime > 0) {
      long start = System.currentTimeMillis();
      saveData(window, maxTime);
      logger.debug("Save: Window: {}, Time: {}", window, System.currentTimeMillis() - start);
    }
    try {
      // Delete expired buckets from data structures
      ((BucketStore.ExpirableBucketStore<T>)bucketStore).deleteExpiredBuckets(expiryPoint);
    } catch (IOException e) {
      throw new RuntimeException("Exception in deleting buckets", e);
    }
    // Record files to be deleted
    ((BucketStore.ExpirableBucketStore<T>)bucketStore).captureFilesToDelete(window);
  }

  /**
   * Sets the bucket span. The span is assumed to be a long value in the domain of the expiry key.
   *
   * @param bucketSpan
   */
  public void setBucketSpan(long bucketSpan)
  {
    this.bucketSpan = bucketSpan;
    recomputeNumBuckets();
  }

  /**
   * Returns the bucket span. The span is assumed to be a long value in the domain of the expiry key.
   *
   * @return bucketSpan
   */
  public long getBucketSpan()
  {
    return bucketSpan;
  }

  /**
   * Sets the expiry period for the incoming data. The expiry period is assumed to be a long value in the domain of the
   * expiry key. Additionally recalculates the number of buckets and expiry end points.
   *
   * @param expiryPeriod
   */
  public void setExpiryPeriod(long expiryPeriod)
  {
    this.expiryPeriod = expiryPeriod;
    recomputeNumBuckets();
  }

  /**
   * Returns the period of expiry for the incoming data. Expiry period is expected to be a long value.
   *
   * @return expiryPeriod
   */
  public long getExpiryPeriod()
  {
    return expiryPeriod;
  }

  /**
   * Returns the maximum period that an incoming tuple can jump ahead of the current endOfBuckets value.
   *
   * @return maxExpiryJump
   */
  public long getMaxExpiryJump()
  {
    return maxExpiryJump;
  }

  /**
   * Sets the maximum period that an incoming tuple can jump ahead of the current endOfBuckets value.
   *
   * @param maxExpiryJump
   */
  public void setMaxExpiryJump(long maxExpiryJump)
  {
    this.maxExpiryJump = maxExpiryJump;
  }

  public static enum CounterKeys
  {
    LOW, HIGH, IGNORED_EVENTS
  }

  private static final transient Logger logger = LoggerFactory.getLogger(AbstractOrderedBucketManager.class);

}
