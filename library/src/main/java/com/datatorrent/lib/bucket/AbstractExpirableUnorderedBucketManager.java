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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.datatorrent.lib.bucket.BucketManager.CounterKeys;
import com.datatorrent.lib.counters.BasicCounters;

/**
 * This is the base implementation of OrderedBucketManager which contains all the events which belong to the same bucket.
 * Subclasses must implement the getEventKey method which gets the keys on which deduplication is done.
 *
 * @param <T>
 *
 */
public abstract class AbstractExpirableUnorderedBucketManager<T> extends AbstractBucketManager<T>
{
  public static int DEF_NUM_EXPIRY_BUCKETS = 5;

  protected long expiryPoint;
  protected long[] maxExpiryPerBucket;
  protected List<String> categoricals;
  protected Map<String, Long> accessTimes;
  protected EvictionPolicy policy;
  
  protected final transient Lock lock;

  public AbstractExpirableUnorderedBucketManager()
  {
    super();
    noOfBuckets = DEF_NUM_EXPIRY_BUCKETS;
    categoricals = Lists.newArrayList();
    accessTimes = Maps.newHashMap();
    policy = EvictionPolicy.LRU; // default
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
//    noOfBuckets = (int) expiryBuckets;
    noOfBucketsInMemory = noOfBuckets;
    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxExpiryPerBucket = new long[noOfBuckets];
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
    super.startService(listener);
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    String expiryKey = getExpiryKey(event);
    if(categoricals.contains(expiryKey) && categoricals.indexOf(expiryKey) < expiryPoint){
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
          throw new RuntimeException("Eviction policy "+ policy +" not implemented yet");
        }
        categoricals.add(expiryKey);
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
//    clone.expiryBuckets = expiryBuckets;
    clone.expiryPoint = expiryPoint;
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
//    if (expiryBuckets != that.expiryBuckets) {
//      return false;
//    }
    return policy.name() == that.policy.name();

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
//    result = 31 * result + (int) (noOfBuckets ^ (expiryBuckets >>> 32));
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
//    try {
//      ((BucketStore.ExpirableBucketStore<T>) bucketStore).deleteExpiredBuckets(accessTimes.get(categoricals.indexOf(expiryPoint)));
//    }
//    catch (IOException e) {
//      throw new RuntimeException(e);
//    }
  }

  private static class Lock
  {
  }

  public static enum CounterKeys
  {
    LOW, HIGH
  }
  public static enum EvictionPolicy
  {
    LRU, FIFO;
  }
  
//  public long getExpiryBuckets()
//  {
//    return expiryBuckets;
//  }
//
//  public void setExpiryBuckets(long expiryBuckets)
//  {
//    this.expiryBuckets = expiryBuckets;
//  }
//
  public EvictionPolicy getPolicy()
  {
    return policy;
  }

  public void setPolicy(EvictionPolicy policy)
  {
    this.policy = policy;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractExpirableUnorderedBucketManager.class);

}
