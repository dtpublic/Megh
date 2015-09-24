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

import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base implementation of TimeBasedBucketManager.
 * Subclasses must implement the getTime method which gets the time field of the incoming tuple.
 * The expiry key (time field) is assumed to be an ordered numeric field (time in seconds) based on which we can create buckets and expire incoming tuples.
 *
 * @param <T>
 */
public abstract class AbstractExpirableTimeBasedBucketManager<T> extends AbstractExpirableOrderedBucketManager<T>
{
  /**
   * Signifies whether the System time or the tuple time is used to advance the expiry point.
   */
  private boolean useSystemTime = true;

  public AbstractExpirableTimeBasedBucketManager()
  {
    super();
  }

  /**
   * Gets the value of the time field of the tuple.
   * Assumes that the key returned will be the time field in milliseconds.
   *
   * @param event
   * @return value of time field in event in milliseconds
   */
  protected abstract long getTime(T event);

  @Deprecated
  @Override
  public AbstractExpirableTimeBasedBucketManager<T> cloneWithProperties()
  {
    return null;
  }

  @Override
  protected void recomputeNumBuckets()
  {
    if(useSystemTime){ //system time
      Calendar calendar = Calendar.getInstance();
      long now = calendar.getTimeInMillis() / 1000;
      calendar.add(Calendar.SECOND, (int) -expiryPeriod);
      startOfBuckets = calendar.getTimeInMillis() / 1000;
      expiryPoint = startOfBuckets;
      noOfBuckets = (int) Math.ceil(( now - startOfBuckets ) / ( bucketSpan * 1.0 )) + 1;
      logger.debug("Now {}, StartOfBuckets {}, Expiry Point {}, Num Buckets {}, Expiry Period: {}", now, startOfBuckets, expiryPoint, noOfBuckets, expiryPeriod);
    }
    else{ // tuple time
      startOfBuckets = 0;
      expiryPoint = startOfBuckets;
      noOfBuckets = (int) Math.ceil((expiryPeriod) / (bucketSpan * 1.0)) + 1;
    }
    deleteExpiryPoint = expiryPoint;

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
    if(useSystemTime)
    {
      synchronized (lock)
      { // Update expiry
        endOfBuckets = System.currentTimeMillis() / 1000;
        expiryPoint = endOfBuckets - expiryPeriod + 1;
        if (recordStats) {
          End_Of_Buckets = endOfBuckets;
          Start_Of_Buckets = expiryPoint;
        }
      }
    }
    else {
      super.processExpiry(expiryKey);
    }
  }

  @Override
  public void shutdownService()
  {
    super.shutdownService();
  }

  @Override
  public AbstractExpirableTimeBasedBucketManager<T> clone() throws CloneNotSupportedException
  {
    AbstractExpirableTimeBasedBucketManager<T> clone = (AbstractExpirableTimeBasedBucketManager<T>)super.clone();
    clone.useSystemTime = useSystemTime;
    return clone;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractExpirableTimeBasedBucketManager)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    AbstractExpirableTimeBasedBucketManager<T> that = (AbstractExpirableTimeBasedBucketManager<T>)o;
    return useSystemTime == that.useSystemTime;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (useSystemTime ? 1 : 0);
    return result;
  }

  /**
   * Check whether to use System time to advance expiry point.
   * If set to false, Tuple time will be used to advance the expiry point, else System time will be used.
   *
   * @return boolean use system time
   */
  public boolean isUseSystemTime()
  {
    return useSystemTime;
  }

  /**
   * Sets whether to use System time to advance expiry point.
   * If set to false, Tuple time will be used to advance the expiry point, else System time will be used.
   *
   * @param useSystemTime
   */
  public void setUseSystemTime(boolean useSystemTime)
  {
    this.useSystemTime = useSystemTime;
  }

  /**
   * Returns the expiry key based on which the buckets will be formed and incoming tuples will be expired.
   * Expiry key is expected to be in seconds.
   *
   * @param event
   * @return long expiry key - in seconds
   */
  protected long getExpiryKey(T event)
  {
    return getTime(event) / 1000;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractExpirableTimeBasedBucketManager.class);

}
