/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This is the base implementation of UnorderedBucketManager. Subclasses must implement the getExpiryKey method which
 * gets the keys on which expiry is done. The expiry key is assumed to be a categorical key and to implicitly have no
 * ordering within its domain.
 *
 * @param <T>
 *
 */
public abstract class AbstractCategoricalBucketManager<T> extends AbstractBucketManager<T>
{
  // Defaults
  public static int DEF_NUM_EXPIRY_BUCKETS = 1;
  public static ExpiryPolicy DEF_EXPIRY_POLICY = ExpiryPolicy.FIFO;

  // Checkpointed state
  protected long expiryPoint;
  /**
   * Number of buckets to be maintained concurrently. Each expiry key maps to an expiry bucket.
   */
  @Min(1)
  protected int expiryBuckets;
  /**
   * Max expiry per bucket. This will function as per the usual bucket semantics. This does not know what index maps to
   * what bucket/ expiry key.
   */
  protected long[] maxExpiryPerBucket;
  /**
   * The list of all the expiry keys seen. This is responsible for maintaining the transparency about categorical expiry
   * keys. The index in this list will act as the bucket key for the corresponding categorical expiry key.
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

  public AbstractCategoricalBucketManager()
  {
    super();
    expiryBuckets = DEF_NUM_EXPIRY_BUCKETS;
    categoricals = Lists.newArrayList();
    accessTimes = Maps.newHashMap();
    policy = DEF_EXPIRY_POLICY; // default
  }

  /**
   * Returns the value of the expiry key in the tuple. The value is expected to be of type String.
   *
   * @param event
   * @return String expiryKey
   */
  protected abstract String getExpiryKey(T event);

  /**
   * Returns the value of the dedup key in the tuple.
   *
   * @param event
   * @return dedupKey
   */
  protected abstract Object getEventKey(T event);

  @Deprecated
  @Override
  public AbstractCategoricalBucketManager<T> cloneWithProperties()
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
    expiryPoint = -1;

    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxExpiryPerBucket = new long[noOfBuckets];
    logger.debug("Num Expiry Buckets {}, No Of Buckets: {}", expiryBuckets, noOfBuckets);
  }

  /**
   * Returns the bucket key for the incoming tuple. The bucket key is expected to be a long value. Check if the expiry
   * key is expired. If not, add it to categoricals and accessTimes data structures. If max number of expiry buckets are
   * reached, then expire one of the existing ones using the expiry policy: FIFI or LRU
   *
   * @return long bucket key
   */
  @Override
  public long getBucketKeyFor(T event)
  {
    String expiryKey = getExpiryKey(event);
    if (categoricals.contains(expiryKey) && categoricals.indexOf(expiryKey) < expiryPoint) { // Check expired
      return -1;
    }

    if (categoricals.contains(expiryKey)) {
      return Math.abs(getEventKey(event).hashCode()) % noOfBuckets;
    } else { // New key
      if (accessTimes.keySet().size() < expiryBuckets) {
        categoricals.add(expiryKey);
        accessTimes.put(expiryKey, System.currentTimeMillis());
      } else if (accessTimes.keySet().size() == expiryBuckets) {
        switch (policy) {

          case FIFO:
            int minIndex = Integer.MAX_VALUE;
            for (String key : accessTimes.keySet()) {
              if (categoricals.indexOf(key) < minIndex) {
                minIndex = categoricals.indexOf(key);
              }
            }
            accessTimes.remove(categoricals.get(minIndex)); // Remove earliest entry
            break;

          case LRU:
            long minTime = Long.MAX_VALUE;
            String minTimeKey = "";
            for (Entry<String, Long> keyVal : accessTimes.entrySet()) {
              if (keyVal.getValue() < minTime) {
                minTime = keyVal.getValue();
                minTimeKey = keyVal.getKey();
              }
            }
            accessTimes.remove(minTimeKey); // Remove LRU entry
            break;

          default:
            throw new RuntimeException("Expiry policy " + policy + " not implemented yet");
        }
        categoricals.add(expiryKey); // Add new entry
        accessTimes.put(expiryKey, System.currentTimeMillis()); // Insert new key
      } else {
        throw new RuntimeException("Buckets already greater than expiry buckets");
      }
    }
    expiryPoint = categoricals.size() - 1 >= expiryBuckets ? categoricals.size() - expiryBuckets : 0;

    return Math.abs(getEventKey(event).hashCode()) % noOfBuckets;
  }

  @Override
  public void newEvent(long bucketKey, T event)
  {
    super.newEvent(bucketKey, event);

    int bucketIdx = (int)(bucketKey % noOfBuckets);
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
    try {
      // Delete expired buckets from data structures
      ((BucketStore.ExpirableBucketStore<T>)bucketStore).deleteExpiredBuckets(expiryPoint);
    } catch (IOException e) {
      throw new RuntimeException("Exception in deleting buckets", e);
    }
    // Record files to be deleted
    ((BucketStore.ExpirableBucketStore<T>)bucketStore).captureFilesToDelete(window);
  }

  @Override
  public void committed(long window)
  {
    ((BucketStore.ExpirableBucketStore<T>)bucketStore).committed(window);
  }

  /**
   * Expiry policies FIFO - The expiry key which is seen first will be expired first. LRU - The expiry key which is the
   * least recently accessed among the existing ones, will be expired first.
   */
  public static enum ExpiryPolicy
  {
    LRU, FIFO;
  }

  /**
   * Returns the expiry policy to be used for expiring existing buckets
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
   * Returns the number of expiry buckets to be processed concurrently, before expiring any of the buckets. i.e. number
   * of categoricals to be processed concurrently
   *
   * @return
   */
  public int getExpiryBuckets()
  {
    return expiryBuckets;
  }

  /**
   * Sets the number of expiry buckets i.e. number of categoricals to be processed concurrently
   *
   * @param expiryBuckets
   */
  public void setExpiryBuckets(int expiryBuckets)
  {
    this.expiryBuckets = expiryBuckets;
    recomputeNumBuckets();
  }

  private static final transient Logger logger = LoggerFactory
      .getLogger(AbstractCategoricalBucketManager.class);
}
