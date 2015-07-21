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
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.bucket.AbstractOrderedBucketManager.CounterKeys;

/**
 * This is the base implementation of TimeBasedBucketManager which contains all the events which belong to the same bucket.
 * Subclasses must implement the getEventKey method which gets the keys on which deduplication is done.
 *
 * @param <T>
 *
 * @since 2.1.0
 */
public abstract class AbstractTimeBasedBucketManager<T> extends AbstractOrderedBucketManager<T>
{
  private boolean useSystemTime = true;
  
  public AbstractTimeBasedBucketManager()
  {
    super();
  }

  public boolean isUseSystemTime()
  {
    return useSystemTime;
  }

  public void setUseSystemTime(boolean useSystemTime)
  {
    this.useSystemTime = useSystemTime;
  }

  protected abstract long getExpiryKey(T event);

  @Deprecated
  @Override
  public AbstractTimeBasedBucketManager<T> cloneWithProperties()
  {
    return null;
  }

  @Override
  protected void recomputeNumBuckets()
  {
    if(useSystemTime){
      Calendar calendar = Calendar.getInstance();
      long now = calendar.getTimeInMillis()/1000;
      calendar.add(Calendar.SECOND, (int) -expiryPeriod);
      startOfBuckets = calendar.getTimeInMillis()/1000;
      expiryPoint = startOfBuckets;
      noOfBuckets = (int) Math.ceil((now - startOfBuckets) / (bucketSpan * 1.0)) + 1;
      logger.debug("Now {}, StartOfBuckets {}, Expiry Point {}, Num Buckets {}, Expiry Period: {}", now, startOfBuckets, expiryPoint, noOfBuckets, expiryPeriod);
    }
    else{ // tuple time
      startOfBuckets = 0;
      expiryPoint = startOfBuckets;
      noOfBuckets = (int) Math.ceil((expiryPeriod) / (bucketSpan * 1.0)) + 1;
    }
    noOfBucketsInMemory = noOfBuckets;

    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxExpiryPerBucket = new Long[noOfBuckets];
  }

  @Override
  public void startService(Listener<T> listener)
  {
    recomputeNumBuckets();
    bucketSlidingTimer = new Timer();
    endOfBuckets = expiryPoint + (noOfBuckets * bucketSpan) - 1;
    logger.debug("bucket properties {}, {}", expiryPeriod, bucketSpan);
    logger.debug("bucket time params: start {}, expiry {}, end {}", startOfBuckets, expiryPoint, endOfBuckets);

    if(useSystemTime){
      bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
      {
        @Override
        public void run()
        {
          synchronized (lock) {
            endOfBuckets = System.currentTimeMillis()/1000;
            expiryPoint = endOfBuckets - expiryPeriod + 1;
            if (recordStats) {
              bucketCounters.getCounter(CounterKeys.HIGH).setValue(endOfBuckets);
              bucketCounters.getCounter(CounterKeys.LOW).setValue(expiryPoint);
            }
          }
          //        try {
          //          ((BucketStore.ExpirableBucketStore<T>) bucketStore).deleteExpiredBuckets(time);
          //        }
          //        catch (IOException e) {
          //          throw new RuntimeException(e);
          //        }
        }
      }, 500, 500);
    }
    super.startService(listener);
  }

  @Override
  public void shutdownService()
  {
    if(useSystemTime){
      bucketSlidingTimer.cancel();
    }
    super.shutdownService();
  }

  @Override
  public AbstractTimeBasedBucketManager<T> clone() throws CloneNotSupportedException
  {
    AbstractTimeBasedBucketManager<T> clone = (AbstractTimeBasedBucketManager<T>)super.clone();
    clone.useSystemTime = useSystemTime;
    return clone;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractTimeBasedBucketManager)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    AbstractTimeBasedBucketManager<T> that = (AbstractTimeBasedBucketManager<T>)o;
    return useSystemTime == that.useSystemTime;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (useSystemTime ? 1 : 0);
    return result;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractTimeBasedBucketManager.class);

}
