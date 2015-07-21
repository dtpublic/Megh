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
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This is the base implementation of OrderedBucketManager
 * Subclasses must implement the getExpiryKey method which gets the keys on which expiry is done.
 * The expiry key is assumed to be an ordered numeric field based on which we can create buckets and expire incoming tuples.
 *
 * @param <T>
 *
 */
public abstract class AbstractExpirableOrderedBucketManager<T> extends AbstractBucketManager<T>
{
  // Defaults
  public static long DEF_EXPIRY_PERIOD = 10000;
  public static long DEF_BUCKET_SPAN = 100;
  public static long DEF_CLEANUP_TIME_MILLIS = 500;

  // Checkpointed state
  protected long cleanupTimeInMillis;
  protected long expiryPeriod;
  @Min(1)
  protected long bucketSpan;
  @Min(0)
  protected long startOfBuckets;
  protected long expiryPoint;
  protected long[] maxExpiryPerBucket;
  protected long deleteExpiryPoint;

  // Un-checkpointed state
  protected transient long endOfBuckets;
  protected transient Timer bucketSlidingTimer;
  protected final transient Lock lock;

  public AbstractExpirableOrderedBucketManager()
  {
    super();
    expiryPeriod = DEF_EXPIRY_PERIOD;
    bucketSpan = DEF_BUCKET_SPAN;
    cleanupTimeInMillis = DEF_CLEANUP_TIME_MILLIS;
    lock = new Lock();
  }

  /**
   * Sub classes implementing this method will return the expiry key of the incoming tuple.
   * The expiry key is expected to be a long key which can be used for expiry.
   *
   * @param event
   * @return long expiry key of tuple
   */
  protected abstract long getExpiryKey(T event);

  @Deprecated
  @Override
  public AbstractExpirableOrderedBucketManager<T> cloneWithProperties()
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
    deleteExpiryPoint = expiryPoint;
    noOfBuckets = (int) Math.ceil((expiryPeriod) / (bucketSpan * 1.0)) + 1;

    //FIXME: This is only for skipping eviction which is a costly operation
    //noOfBucketsInMemory = noOfBuckets;

    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxExpiryPerBucket = new long[noOfBuckets];
  }

  @Override
  public void startService(Listener<T> listener)
  {
    recomputeNumBuckets();
    bucketSlidingTimer = new Timer();
    endOfBuckets = expiryPoint + expiryPeriod -1;
    logger.debug("Bucket Parameters: Expiry Period {}, Bucket Span {}", expiryPeriod, bucketSpan);
    logger.debug("Bucket Manager Properties: start {}, end {}, expiry {}", startOfBuckets, endOfBuckets, expiryPoint);

    // Deletes expired buckets at regular intervals
    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        try {
          ((BucketStore.ExpirableBucketStore<T>) bucketStore).deleteExpiredBuckets(deleteExpiryPoint);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }, cleanupTimeInMillis, cleanupTimeInMillis);

    super.startService(listener);
  }

  /**
   * Gets the bucket key for the incoming tuple
   * Bucket key is expected to be a long and based on the expiry key of the tuple
   *
   * @return long bucket key
   */
  @Override
  public long getBucketKeyFor(T event)
  {
    long expiryKey = getExpiryKey(event);
    if (expiryKey < expiryPoint) {
      return -1;
    }

    // Move expiry point and end of buckets
    synchronized (lock) {
      if (expiryKey > endOfBuckets) {
        //long move = expiryKey - endOfBuckets;
        endOfBuckets = expiryKey;
        expiryPoint = endOfBuckets - expiryPeriod + 1;
        if (recordStats) {
          End_Of_Buckets = endOfBuckets;
          Start_Of_Buckets = expiryPoint;
        }
      }
    }

    long key = expiryKey / bucketSpan;
    return key;
  }

  @Override
  public void endWindow(long window)
  {
    long maxTime = -1;
    for (int bucketIdx : dirtyBuckets.keySet()) {
      if (maxExpiryPerBucket[bucketIdx] > maxTime) {
        maxTime = maxExpiryPerBucket[bucketIdx];
      }
      maxExpiryPerBucket[bucketIdx] = 0;
    }
    if (maxTime > -1) {
      saveData(window, maxTime);
    }
    deleteExpiryPoint = expiryPoint;
  }

  @Override
  public void shutdownService()
  {
    bucketSlidingTimer.cancel();
    super.shutdownService();
  }

  @Override
  public AbstractExpirableOrderedBucketManager<T> clone() throws CloneNotSupportedException
  {
    AbstractExpirableOrderedBucketManager<T> clone = (AbstractExpirableOrderedBucketManager<T>)super.clone();
    clone.bucketSpan = bucketSpan;
    clone.startOfBuckets = startOfBuckets;
    clone.endOfBuckets = endOfBuckets;
    clone.expiryPeriod = expiryPeriod;
    clone.expiryPoint = expiryPoint;
    clone.deleteExpiryPoint = deleteExpiryPoint;
    clone.cleanupTimeInMillis = cleanupTimeInMillis;
    clone.maxExpiryPerBucket = maxExpiryPerBucket.clone();
    return clone;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractExpirableOrderedBucketManager)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    AbstractExpirableOrderedBucketManager<T> that = (AbstractExpirableOrderedBucketManager<T>)o;
    if (! Arrays.equals(maxExpiryPerBucket, that.maxExpiryPerBucket)) {
      return false;
    }
    if (bucketSpan != that.bucketSpan) {
      return false;
    }
    if (expiryPeriod != that.expiryPeriod) {
      return false;
    }
    if (startOfBuckets != that.startOfBuckets) {
      return false;
    }
    if (endOfBuckets != that.endOfBuckets) {
      return false;
    }
    if (cleanupTimeInMillis != that.cleanupTimeInMillis) {
      return false;
    }
    if (deleteExpiryPoint != that.deleteExpiryPoint) {
      return false;
    }
    return expiryPoint == that.expiryPoint;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (int) (bucketSpan ^ (bucketSpan >>> 32));
    result = 31 * result + (int) (startOfBuckets ^ (startOfBuckets >>> 32));
    result = 31 * result + (int) (endOfBuckets ^ (endOfBuckets >>> 32));
    result = 31 * result + (int) (expiryPeriod ^ (expiryPeriod >>> 32));
    result = 31 * result + (int) (expiryPoint ^ (expiryPoint >>> 32));
    result = 31 * result + (int) (deleteExpiryPoint ^ (deleteExpiryPoint >>> 32));
    result = 31 * result + (int) (cleanupTimeInMillis ^ (cleanupTimeInMillis >>> 32));
    return result;
  }

  @Override
  public void newEvent(long bucketKey, T event)
  {
    super.newEvent(bucketKey, event);

    int bucketIdx = (int) (bucketKey % noOfBuckets);
    Long max = maxExpiryPerBucket[bucketIdx];
    long expiryKey = getExpiryKey(event);
    if (max == 0 || expiryKey > max) {
      maxExpiryPerBucket[bucketIdx] = expiryKey;
    }
  }

  private static class Lock
  {
  }

  public static enum CounterKeys
  {
    LOW, HIGH
  }

  /**
   * Gets the max times per bucket. This is the maximum time when a bucket was last accessed.
   *
   * @return long[] of max times per bucket
   */
  public long[] getMaxTimesPerBuckets()
  {
    return maxExpiryPerBucket;
  }

  /**
   * Sets the portion of domain of the expiry key that a bucket spans
   *
   * @param bucketSpan
   */
  public void setBucketSpan(long bucketSpan)
  {
    this.bucketSpan = bucketSpan;
    recomputeNumBuckets();
  }

  /**
   * Gets the portion of domain of the expiry key that a bucket spans
   *
   * @return bucketSpanInMillis
   */
  public long getBucketSpan()
  {
    return bucketSpan;
  }

  /**
   * Gets the period of expiry for the incoming data
   * Period is expected to be a long after passing of which the tuple expires
   *
   * @return long expiry period for the input data
   */
  public long getExpiryPeriod()
  {
    return expiryPeriod;
  }

  /**
   * Gets the period of expiry for the incoming data
   *
   * @param expiryPeriod
   */
  public void setExpiryPeriod(long expiryPeriod)
  {
    this.expiryPeriod = expiryPeriod;
    recomputeNumBuckets();
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractExpirableOrderedBucketManager.class);

}
