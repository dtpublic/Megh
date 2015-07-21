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
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This is the base implementation of UnorderedBucketManager.
 * Subclasses must implement the getExpiryKey method which gets the keys on which expiry is done.
 * The expiry key is assumed to be a categorical key and to implicitly have no ordering within its domain.
 *
 * @param <T>
 *
 */
public abstract class AbstractExpirableUnorderedBucketManager<T> extends AbstractBucketManager<T>
{
  // Defaults
  public static int DEF_NUM_EXPIRY_BUCKETS = 1;
  public static ExpiryPolicy DEF_EXPIRY_POLICY = ExpiryPolicy.FIFO;

  // Checkpointed state
  protected long expiryPoint;
  /**
   * Number of buckets to be maintained concurrently.
   * Each expiry key maps to an expiry bucket.
   */
  @Min(1)
  protected int expiryBuckets;
  /**
   * Max expiry per bucket. This will function as per the usual bucket semantics.
   * This does not know what index maps to what bucket/ expiry key.
   */
  protected long[] maxExpiryPerBucket;
  /**
   * The list of all the expiry keys seen.
   * This is responsible for maintaining the transparency about categorical expiry keys.
   * The index in this list will act as the bucket key for the corresponding categorical expiry key.
   */
  protected List<String> categoricals;
  /**
   * Access times for each expiry key. Each expiry key is categorical in this case.
   */
  protected Map<String, Long> accessTimes;
  /**
   * Expiry policy
   */
  protected ExpiryPolicy policy;
  /**
   * The cleanup time in milliseconds.
   * The cleanup thread which deletes expired buckets will use this time as the run interval.
   */
  protected long cleanupTimeInMillis;

  /**
   * This is the expiry point as the cutoff for deleting all buckets accessed before this time.
   * This will be set only at end window so that the deletion thread does not delete buckets in use in the existing window
   */
  protected long deleteExpiryPoint;

  // Non-checkpointed state
  protected transient Timer bucketSlidingTimer;
  protected final transient Lock lock;

  public AbstractExpirableUnorderedBucketManager()
  {
    super();
    noOfBuckets = DEF_NUM_EXPIRY_BUCKETS;
    categoricals = Lists.newArrayList();
    accessTimes = Maps.newHashMap();
    policy = DEF_EXPIRY_POLICY; // default
    lock = new Lock();
  }

  protected abstract String getExpiryKey(T event);

  @Deprecated
  @Override
  public AbstractExpirableUnorderedBucketManager<T> cloneWithProperties()
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
    noOfBuckets = expiryBuckets;

    //FIXME: Setting to avoid evicting buckets
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

    // Delete expired buckets at regular intervals
    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        try {
          logger.debug("Delete started from thread. Delete Expiry Point: {}", deleteExpiryPoint);
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
   * Gets the bucket key for the incoming tuple. The bucket key is expected to be a long value.
   * Check if the expiry key is expired. If not, add it to categoricals and accessTimes data structures.
   * If max number of expiry buckets are reached, then expire one of the existing ones using the expiry policy: FIFI or LRU
   *
   * @return long bucket key
   */
  @Override
  public long getBucketKeyFor(T event)
  {
    String expiryKey = getExpiryKey(event);
    if(categoricals.contains(expiryKey) && categoricals.indexOf(expiryKey) < expiryPoint){ // Check expired
      return -1;
    }

    if(categoricals.contains(expiryKey)){
      return categoricals.indexOf(expiryKey);
    }
    else { // New key
      if(accessTimes.keySet().size() < noOfBuckets){
        categoricals.add(expiryKey);
        accessTimes.put(expiryKey, System.currentTimeMillis());
      }
      else if(accessTimes.keySet().size() == noOfBuckets){
        switch (policy) {

        case FIFO:
          int minIndex = Integer.MAX_VALUE;
          for(String key: accessTimes.keySet()){
            if(categoricals.indexOf(key) < minIndex){
              minIndex = categoricals.indexOf(key);
            }
          }
          accessTimes.remove(categoricals.get(minIndex)); // Remove earliest entry
          break;

        case LRU:
          long minTime = Long.MAX_VALUE;
          String minTimeKey = "";
          for(Entry<String, Long> keyVal: accessTimes.entrySet()){
            if(keyVal.getValue() < minTime){
              minTime = keyVal.getValue();
              minTimeKey = keyVal.getKey();
            }
          }
          accessTimes.remove(minTimeKey); // Remove LRU entry
          break;

        default:
          throw new RuntimeException("Expiry policy "+ policy +" not implemented yet");
        }
        categoricals.add(expiryKey); // Add new entry
        accessTimes.put(expiryKey, System.currentTimeMillis()); // Insert new key
      }
      else{
        throw new RuntimeException("Buckets already greater than expiry buckets");
      }
    }
    expiryPoint = categoricals.size()-1 >= noOfBuckets ? categoricals.size() - noOfBuckets : 0;
    return categoricals.indexOf(expiryKey);
  }

  @Override
  public void shutdownService()
  {
    super.shutdownService();
  }

  @Override
  public AbstractExpirableUnorderedBucketManager<T> clone() throws CloneNotSupportedException
  {
    AbstractExpirableUnorderedBucketManager<T> clone = (AbstractExpirableUnorderedBucketManager<T>)super.clone();
    clone.maxExpiryPerBucket = maxExpiryPerBucket.clone();
    clone.accessTimes = Maps.newHashMap(accessTimes);
    clone.policy = policy;
    clone.expiryBuckets = expiryBuckets;
    clone.expiryPoint = expiryPoint;
    clone.deleteExpiryPoint = deleteExpiryPoint;
    clone.categoricals = Lists.newArrayList(categoricals);
    return clone;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractExpirableUnorderedBucketManager)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    AbstractExpirableUnorderedBucketManager<T> that = (AbstractExpirableUnorderedBucketManager<T>)o;
    if ( ! categoricals.containsAll(that.categoricals)) {
      return false;
    }
    if ( ! accessTimes.keySet().containsAll(that.accessTimes.keySet())) {
      return false;
    }
    if (! Arrays.equals(maxExpiryPerBucket, that.maxExpiryPerBucket)) {
      return false;
    }
    if (expiryPoint != that.expiryPoint) {
      return false;
    }
    if (deleteExpiryPoint != that.deleteExpiryPoint) {
      return false;
    }
    return policy.name() == that.policy.name();

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
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
    long max = maxExpiryPerBucket[bucketIdx];
    String expiryKey = getExpiryKey(event);
    long eventTime = accessTimes.get(expiryKey);
    if (max == 0 || eventTime > max) {
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
      maxExpiryPerBucket[bucketIdx] = 0;
    }
    if (maxTime > -1) {
      saveData(window, maxTime);
    }
  }

  private static class Lock
  {
  }

  /**
   * Expiry policies
   * FIFO - The expiry key which is seen first will be expired first
   * LRU - The expiry key which is the least recently accessed among the existing ones, will be expired first.
   */
  public static enum ExpiryPolicy
  {
    LRU, FIFO;
  }

  /**
   * Gets the expiry policy to be used for expiring existing buckets
   *
   * @return expiry policy
   */
  public ExpiryPolicy getPolicy()
  {
    return policy;
  }

  /**
   * Sets the expiry policy
   *
   * @param policy
   */
  public void setPolicy(ExpiryPolicy policy)
  {
    this.policy = policy;
  }

  /**
   * Gets the number of expiry buckets to be processed concurrently, before expiring any of the buckets
   *
   * @return
   */
  public int getExpiryBuckets()
  {
    return expiryBuckets;
  }

  /**
   * Sets the number of expiry buckets
   *
   * @param expiryBuckets
   */
  public void setExpiryBuckets(int expiryBuckets)
  {
    this.expiryBuckets = expiryBuckets;
    recomputeNumBuckets();
  }

  /**
   * Get the cleanup time in milliseconds.
   * The cleanup thread which deletes expired buckets will use this time as the run interval.
   *
   * @return long cleanup time in milliseconds
   */
  public long getCleanupTimeInMillis()
  {
    return cleanupTimeInMillis;
  }

  /**
   * Sets the cleanup time in milliseconds
   *
   * @param cleanupTimeInMillis
   */
  public void setCleanupTimeInMillis(long cleanupTimeInMillis)
  {
    this.cleanupTimeInMillis = cleanupTimeInMillis;
  }

  /**
   * Gets the expiry point as the cutoff for deleting all buckets accessed before this time.
   * This will be set only at end window so that the deletion thread does not delete buckets in use in the existing window
   *
   * @return long delete expiry point. Same as expiry point, only set in end window.
   */
  public long getDeleteExpiryPoint()
  {
    return deleteExpiryPoint;
  }

  /**
   * Sets the delete expiry point
   *
   * @param deleteExpiryPoint
   */
  public void setDeleteExpiryPoint(long deleteExpiryPoint)
  {
    this.deleteExpiryPoint = deleteExpiryPoint;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractExpirableUnorderedBucketManager.class);

}
