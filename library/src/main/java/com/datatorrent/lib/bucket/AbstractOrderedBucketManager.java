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
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.counters.BasicCounters;

/**
 * This is the base implementation of TimeBasedBucketManager which contains all the events which belong to the same bucket.
 * Subclasses must implement the getEventKey method which gets the keys on which deduplication is done.
 *
 * @param <T>
 *
 * @since 2.1.0
 */
public abstract class AbstractOrderedBucketManager<T> extends AbstractBucketManager<T>
{
  public static long DEF_EXPIRY_PERIOD = 10000;
  public static long DEF_BUCKET_SPAN = 100;
  public static long DEF_CLEANUP_TIME_MILLIS = 500;

  protected long cleanUpTimeMillis = DEF_CLEANUP_TIME_MILLIS;
  protected long expiryPeriod;
  @Min(1)
  protected long bucketSpan;
  @Min(0)
  protected long startOfBuckets;
  protected long expiryPoint;
  protected Long[] maxExpiryPerBucket;

  protected transient long endOfBuckets;
  protected transient Timer bucketSlidingTimer;
  protected final transient Lock lock;

  public AbstractOrderedBucketManager()
  {
    super();
    expiryPeriod = DEF_EXPIRY_PERIOD;
    bucketSpan = DEF_BUCKET_SPAN;
    lock = new Lock();
  }

  protected abstract long getExpiryKey(T event);

  public Long[] getMaxTimesPerBuckets()
  {
    return maxExpiryPerBucket;
  }

  /**
   * Sets the number of milliseconds a bucket spans.
   *
   * @param bucketSpan
   */
  public void setBucketSpan(long bucketSpan)
  {
    this.bucketSpan = bucketSpan;
    recomputeNumBuckets();
  }

  /**
   * Gets the number of milliseconds a bucket spans.
   *
   * @return bucketSpanInMillis
   */
  public long getBucketSpan()
  {
    return bucketSpan;
  }

  public long getExpiryPeriod()
  {
    return expiryPeriod;
  }

  public void setExpiryPeriod(long expiryPeriod)
  {
    this.expiryPeriod = expiryPeriod;
    recomputeNumBuckets();
  }

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
    startOfBuckets = 0; //calendar.getTimeInMillis();
    expiryPoint = startOfBuckets;
    noOfBuckets = (int) Math.ceil((expiryPeriod) / (bucketSpan * 1.0)) + 1;
    noOfBucketsInMemory = noOfBuckets;
    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxExpiryPerBucket = new Long[noOfBuckets];
  }

  @Override
  public void setBucketCounters(@Nonnull BasicCounters<MutableLong> bucketCounters)
  {
    super.setBucketCounters(bucketCounters);
    bucketCounters.setCounter(CounterKeys.LOW, new MutableLong());
    bucketCounters.setCounter(CounterKeys.HIGH, new MutableLong());
  }

  @Override
  public void startService(Listener<T> listener)
  {
    recomputeNumBuckets();
    bucketSlidingTimer = new Timer();
    endOfBuckets = expiryPoint + (noOfBuckets * bucketSpan) - 1;
    logger.debug("bucket properties {}, {}", expiryPeriod, bucketSpan);
    logger.debug("bucket time params: start {}, expiry {}, end {}", startOfBuckets, expiryPoint, endOfBuckets);

//    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
//    {
//      @Override
//      public void run()
//      {
//        long time;
//        synchronized (lock) {
//          time = (expiryPoint += bucketSpan);
//          endOfBuckets += bucketSpan;
//          if (recordStats) {
//            bucketCounters.getCounter(CounterKeys.HIGH).setValue(endOfBuckets);
//            bucketCounters.getCounter(CounterKeys.LOW).setValue(expiryPoint);
//          }
//        }
//        try {
//          ((BucketStore.ExpirableBucketStore<T>) bucketStore).deleteExpiredBuckets(expiryPoint);
//        }
//        catch (IOException e) {
//          throw new RuntimeException(e);
//        }
//      }
//
//    }, cleanUpTimeMillis, cleanUpTimeMillis);
    super.startService(listener);
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    long expiryKey = getExpiryKey(event);
    if (expiryKey < expiryPoint) {
      return -1;
    }
    //long diffFromStart = expiryKey - startOfBuckets;
    long key = expiryKey / bucketSpan;
    synchronized (lock) {
      if (expiryKey > endOfBuckets) {
        long move = expiryKey - endOfBuckets; //(((expiryKey - endOfBuckets) / bucketSpan) + 1) * bucketSpan;
        endOfBuckets += move;
        expiryPoint = endOfBuckets - expiryPeriod + 1;
        if (recordStats) {
          bucketCounters.getCounter(CounterKeys.HIGH).setValue(endOfBuckets);
          bucketCounters.getCounter(CounterKeys.LOW).setValue(expiryPoint);
        }
      }
    }
    return key;
  }

  @Override
  public void shutdownService()
  {
    bucketSlidingTimer.cancel();
    super.shutdownService();
  }

  @Override
  public AbstractOrderedBucketManager<T> clone() throws CloneNotSupportedException
  {
    AbstractOrderedBucketManager<T> clone = (AbstractOrderedBucketManager<T>)super.clone();
    clone.maxExpiryPerBucket = maxExpiryPerBucket.clone();
    return clone;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractOrderedBucketManager)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    AbstractOrderedBucketManager<T> that = (AbstractOrderedBucketManager<T>)o;
    if (bucketSpan != that.bucketSpan) {
      return false;
    }
    if (startOfBuckets != that.startOfBuckets) {
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
    result = 31 * result + (int) (expiryPoint ^ (expiryPoint >>> 32));
    return result;
  }

  @Override
  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    AbstractBucket<T> bucket = buckets[bucketIdx];
    logger.debug("Adding event {} to bucket index {}", event, bucketIdx);
    
    if(bucket != null){
      logger.debug("Old Bucket Key {}, Requested {}", bucket.bucketKey, bucketKey);
    }
    
    if (bucket == null || bucket.bucketKey != bucketKey) {
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    }
    else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }

    bucket.addNewEvent(bucket.getEventKey(event), writeEventKeysOnly ? null : event);
    bucketCounters.getCounter(BucketManager.CounterKeys.EVENTS_IN_MEMORY).increment();

    Long max = maxExpiryPerBucket[bucketIdx];
    long eventTime = getExpiryKey(event);
    if (max == null || eventTime > max) {
      maxExpiryPerBucket[bucketIdx] = eventTime;
    }
  }

  @Override
  public void endWindow(long window)
  {
    long maxTime = -1;
    for (int bucketIdx : dirtyBuckets.keySet()) {
      if (maxExpiryPerBucket[bucketIdx] > maxTime) {
        maxTime = maxExpiryPerBucket[bucketIdx];
      }
      maxExpiryPerBucket[bucketIdx] = null;
    }
    if (maxTime > -1) {
      saveData(window, maxTime);
    }
    try {
      ((BucketStore.ExpirableBucketStore<T>) bucketStore).deleteExpiredBuckets(expiryPoint);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class Lock
  {
  }

  public static enum CounterKeys
  {
    LOW, HIGH
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractOrderedBucketManager.class);

}
