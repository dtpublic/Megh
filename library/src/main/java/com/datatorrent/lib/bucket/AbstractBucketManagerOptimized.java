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

import com.datatorrent.api.AutoMetric;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the base implementation of BucketManager.
 * Subclasses must implement the createBucket method which creates the specific bucket based on bucketKey provided
 * and getBucketKeyFor method which calculates the bucket key of an event.
 * <p>
 * Configurable properties of the AbstractBucketManager.<br/>
 * <ol>
 * <li>
 * {@link #noOfBuckets}: total number of buckets.
 * </li>
 * <li>
 * {@link #noOfBucketsInMemory}: number of buckets that will be kept in memory. This limit is not strictly maintained.<br/>
 * Whenever a new bucket from disk is loaded, the manager checks whether this limit is reached. If it has then it finds the
 * least recently used (lru) bucket. If the lru bucket was accessed within last {@link #millisPreventingBucketEviction}
 * then it is NOT off-loaded otherwise it is removed from memory.
 * </li>
 * <li>
 * {@link #maxNoOfBucketsInMemory}: this is a hard limit that is enforced on number of buckets in the memory.<br/>
 * When this limit is reached, the lru bucket is off-loaded irrespective of its last accessed time.
 * </li>
 * <li>
 * {@link #millisPreventingBucketEviction}: duration in milliseconds which could prevent a bucket from being
 * offloaded.
 * </li>
 * <li>
 * {@link #writeEventKeysOnly}: when this is true, the manager would not cache the event. It will only
 * keep the event key. This reduces memory usage and is useful for operators like De-duplicator which are interested only
 * in the event key.
 * </li>
 * </ol>
 * </p>
 *
 * @param <T> event type
 * @since 0.9.4
 */
public abstract class AbstractBucketManagerOptimized<T> extends AbstractBucketManager<T>
{
  private static final long RESERVED_BUCKET_KEY = -2;

  protected boolean collateFilesForBucket = false;
  protected Map<Integer, AbstractBucket<T>> dirtyBuckets;

  // Metrics
  @AutoMetric
  protected long Deleted_Buckets;
  @AutoMetric
  protected long Buckets_In_Memory;
  @AutoMetric
  protected long Evicted_Buckets;
  @AutoMetric
  protected long Events_In_Memory;
  @AutoMetric
  protected long Events_Committed_Last_Window;
  @AutoMetric
  protected long End_Of_Buckets;
  @AutoMetric
  protected long Start_Of_Buckets;

  //Not check-pointed
  @NotNull
  protected transient BlockingQueue<Long> eventQueue;
  private transient volatile boolean running;
  @NotNull
  protected transient Lock lock;
  @NotNull
  protected transient MinMaxPriorityQueue<AbstractBucket<T>> bucketHeap;

  public AbstractBucketManagerOptimized()
  {
    eventQueue = new LinkedBlockingQueue<Long>();
    evictionCandidates = Sets.newHashSet();
    dirtyBuckets = Maps.newConcurrentMap();
    bucketHeap = MinMaxPriorityQueue.orderedBy(new Comparator<AbstractBucket<T>>()
    {
      @Override
      public int compare(AbstractBucket<T> bucket1, AbstractBucket<T> bucket2)
      {
        if (bucket1.lastUpdateTime() < bucket2.lastUpdateTime()) {
          return -1;
        }
        if (bucket1.lastUpdateTime() > bucket2.lastUpdateTime()) {
          return 1;
        }
        return 0;
      }

    }).create();
    lock = new Lock();
    committedWindow = -1;

    noOfBuckets = DEF_NUM_BUCKETS;
    noOfBucketsInMemory = DEF_NUM_BUCKETS_MEM;
    maxNoOfBucketsInMemory = DEF_NUM_BUCKETS_MEM + 100;
    millisPreventingBucketEviction = DEF_MILLIS_PREVENTING_EVICTION;
    writeEventKeysOnly = true;
  }

  @Override
  public void run()
  {
    running = true;
    List<Long> requestedBuckets = Lists.newArrayList();
    // Evicted Buckets Map: Bucket Index -> Bucket Key.
    Map<Integer, Long> evictedBuckets = Maps.newHashMap();
    try {
      while (running) {
        Long request = eventQueue.poll(1, TimeUnit.SECONDS);
        if (request != null) {
          long requestedKey = request;
          if (RESERVED_BUCKET_KEY == requestedKey) {
            synchronized (lock) {
              lock.notify();
            }
            requestedBuckets.clear();
          }
          else {
            requestedBuckets.add(requestedKey);
            int bucketIdx = (int) (requestedKey % noOfBuckets);
            long numEventsRemoved = 0;

            if (buckets[bucketIdx] != null && buckets[bucketIdx].bucketKey != requestedKey) {
              //Delete the old bucket in memory at that index.
              AbstractBucket<T> oldBucket = buckets[bucketIdx];

              dirtyBuckets.remove(bucketIdx);
              evictionCandidates.remove(bucketIdx);
              buckets[bucketIdx] = null;

              listener.bucketOffLoaded(oldBucket.bucketKey);
              bucketStore.deleteBucket(bucketIdx);
              listener.bucketDeleted(oldBucket.bucketKey);

              if (recordStats) {
                Deleted_Buckets++;
                Buckets_In_Memory--;
                numEventsRemoved += oldBucket.countOfUnwrittenEvents() + oldBucket.countOfWrittenEvents();
              }
              logger.debug("deleted bucket {} {}", oldBucket.bucketKey, bucketIdx);
            }
            else if(buckets[bucketIdx] == null) // May be due to eviction or due to operator crash
            {
              if(evictedBuckets.containsKey(bucketIdx) && evictedBuckets.get(bucketIdx) < requestedKey){
                bucketStore.deleteBucket(bucketIdx);
                logger.debug("deleted bucket positions for idx {}", bucketIdx);
              }
            }

            Map<Object, T> bucketDataInStore = bucketStore.fetchBucket(bucketIdx);

            //Delete the least recently used bucket in memory if the noOfBucketsInMemory threshold is reached.
            if (evictionCandidates.size() + 1 > noOfBucketsInMemory) {

              for (int anIndex : evictionCandidates) {
                bucketHeap.add(buckets[anIndex]);
              }
              int overFlow = evictionCandidates.size() + 1 - noOfBucketsInMemory;
              while (overFlow-- >= 0) {
                AbstractBucket<T> lruBucket = bucketHeap.poll();
                if (lruBucket == null) {
                  break;
                }
                // Do not evict buckets loaded in the current window
                if(requestedBuckets.contains(lruBucket.bucketKey)) {
                  break;
                }
                int lruIdx = (int) (lruBucket.bucketKey % noOfBuckets);

                if (dirtyBuckets.containsKey(lruIdx)) {
                  break;
                }
                if (((System.currentTimeMillis() - lruBucket.lastUpdateTime()) < millisPreventingBucketEviction)
                  && ((evictionCandidates.size() + 1) <= maxNoOfBucketsInMemory)) {
                  break;
                }
                evictionCandidates.remove(lruIdx);
                buckets[lruIdx] = null;
                evictedBuckets.put(lruIdx, lruBucket.bucketKey);
                listener.bucketOffLoaded(lruBucket.bucketKey);
                if (recordStats) {
                  Evicted_Buckets++;
                  Buckets_In_Memory--;
                  numEventsRemoved += lruBucket.countOfUnwrittenEvents() + lruBucket.countOfWrittenEvents();
                }
                logger.debug("evicted bucket {} {}", lruBucket.bucketKey, lruIdx);
              }
            }

            AbstractBucket<T> bucket = buckets[bucketIdx];
            if (bucket == null || bucket.bucketKey != requestedKey) {
              bucket = createBucket(requestedKey);
              buckets[bucketIdx] = bucket;
              evictedBuckets.remove(bucketIdx);
            }
            bucket.setWrittenEvents(bucketDataInStore);
            evictionCandidates.add(bucketIdx);
            listener.bucketLoaded(bucket);
            if (recordStats) {
              Buckets_In_Memory++;
              Events_In_Memory += bucketDataInStore.size() - numEventsRemoved;
            }
            bucketHeap.clear();
          }
        }
      }
    }
    catch (Throwable cause) {
      running = false;
      DTThrowable.rethrow(cause);
    }
  }

  @Override
  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    AbstractBucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    }
    else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }

    bucket.addNewEvent(bucket.getEventKey(event), writeEventKeysOnly ? null : event);
    if (recordStats) {
      Events_In_Memory++;
    }
  }

  protected void saveData(long window, long id)
  {
    Map<Integer, Map<Object, T>> dataToStore = Maps.newHashMap();
    long eventsCount = 0;
    for (Map.Entry<Integer, AbstractBucket<T>> entry : dirtyBuckets.entrySet()) {
      AbstractBucket<T> bucket = entry.getValue();
      dataToStore.put(entry.getKey(), bucket.getUnwrittenEvents());
      eventsCount += bucket.countOfUnwrittenEvents();

      if(collateFilesForBucket){
        if(bucket.getWrittenEventKeys() != null && !bucket.getWrittenEventKeys().isEmpty()) // Then collate all data together and write into a new file together
        {
          dataToStore.put(entry.getKey(), bucket.getWrittenEvents());
          try {
            bucketStore.deleteBucket(entry.getKey());
          } catch (IOException e) {
            throw new RuntimeException("Exception while deleting bucket", e);
          } // Delete pointers to previous window files
        }
      }
      bucket.transferDataFromMemoryToStore();
      evictionCandidates.add(entry.getKey());
    }

    if (recordStats) {
      Events_Committed_Last_Window = eventsCount;
    }

    try {
      if (!dataToStore.isEmpty()) {
        long start = System.currentTimeMillis();
        logger.debug("start store {}", window);
        bucketStore.storeBucketData(window, id, dataToStore);
        logger.debug("end store {} num {} buckets {} took {}", window, eventsCount, dataToStore.size(), System.currentTimeMillis() - start);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    dirtyBuckets.clear();
    committedWindow = window;
  }

  @Override
  public void blockUntilAllRequestsServiced() throws InterruptedException
  {
    synchronized (lock) {
      loadBucketData(RESERVED_BUCKET_KEY);
      lock.wait();
    }
  }

  @Override
  public void loadBucketData(long bucketKey)
  {
    eventQueue.offer(bucketKey);
  }

  public boolean isCollateFilesForBucket()
  {
    return collateFilesForBucket;
  }

  public void setCollateFilesForBucket(boolean collateFilesForBucket)
  {
    this.collateFilesForBucket = collateFilesForBucket;
  }

  // Getters for Bucket Metrics
  public long getDeleted_Buckets()
  {
    return Deleted_Buckets;
  }
  public long getEvicted_Buckets()
  {
    return Evicted_Buckets;
  }
  public long getEvents_In_Memory()
  {
    return Events_In_Memory;
  }
  public long getEvents_Committed_Last_Window()
  {
    return Events_Committed_Last_Window;
  }
  public long getEnd_Of_Buckets()
  {
    return End_Of_Buckets;
  }
  public long getStart_Of_Buckets()
  {
    return Start_Of_Buckets;
  }

  // Getters for Bucket Metrics
  public void setDeleted_Buckets(long deleted_Buckets)
  {
    Deleted_Buckets = deleted_Buckets;
  }
  public void setBuckets_In_Memory(long buckets_In_Memory)
  {
    Buckets_In_Memory = buckets_In_Memory;
  }
  public void setEvicted_Buckets(long evicted_Buckets)
  {
    Evicted_Buckets = evicted_Buckets;
  }
  public void setEvents_In_Memory(long events_In_Memory)
  {
    Events_In_Memory = events_In_Memory;
  }
  public void setEvents_Committed_Last_Window(long events_Committed_Last_Window)
  {
    Events_Committed_Last_Window = events_Committed_Last_Window;
  }
  public void setEnd_Of_Buckets(long end_Of_Buckets)
  {
    End_Of_Buckets = end_Of_Buckets;
  }
  public void setStart_Of_Buckets(long start_Of_Buckets)
  {
    Start_Of_Buckets = start_Of_Buckets;
  }

  private static class Lock
  {
  }

  private static transient final Logger logger = LoggerFactory.getLogger(AbstractBucketManagerOptimized.class);
}