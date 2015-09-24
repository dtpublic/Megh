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
package com.datatorrent.lib.dedup;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.AbstractBucketManager;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.bloomFilter.BloomFilter;
import com.datatorrent.api.*;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.netlet.util.DTThrowable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This is the base implementation of an deduper.&nbsp;
 * This deduper determines whether a duplicate event has occurred and spools data out to a particular store as necessary,
 * Subclasses must implement the getEventKey method which gets the keys on which deduplication is done
 * and convert method which turns input tuples into output tuples.
 *  <p>
 * Processing of an event involves:
 * <ol>
 * <li>Finding the bucket key of an event by calling {@link BucketManager#getBucketKeyFor(Bucketable)}.</li>
 * <li>Getting the bucket from {@link BucketManager} by calling {@link BucketManager#getBucket(long)}.</li>
 * <li>
 * If the bucket is not loaded:
 * <ol>
 * <li>it requests the {@link BucketManager} to load the bucket which is a non-blocking call.</li>
 * <li>Adds the event to {@link #waitingEvents} which is a collection of events that are waiting for buckets to be loaded.</li>
 * <li>{@link BucketManager} loads the bucket and informs deduper by calling {@link #bucketLoaded(Bucket)}</li>
 * <li>The deduper then processes the waiting events in {@link #handleIdleTime()}</li>
 * </ol>
 * <li>
 * If the bucket is loaded, the operator drops the event if it is already present in the bucket; emits it otherwise.
 * </li>
 * </ol>
 * </p>
 *
 * <p>
 * Based on the assumption that duplicate events fall in the same bucket.
 * </p>
 *
 * @displayName Deduper
 * @category Deduplication
 * @tags dedupe
 *
 * @param <INPUT>  type of input tuple
 * @param <OUTPUT> type of output tuple
 * @since 0.9.4
 */
public abstract class AbstractDeduper<INPUT, OUTPUT> implements Operator, BucketManager.Listener<INPUT>, Operator.IdleTimeHandler, Partitioner<AbstractDeduper<INPUT, OUTPUT>>, Operator.CheckpointListener, StatsListener
{
  static long DEFAULT_MIN_THRESH_DYNAMIC_PARTITION = 5;
  static long DEFAULT_MAX_THRESH_DYNAMIC_PARTITION = 5000;
  static long DEFAULT_MILLIS_BEFORE_NEXT_DYNAMIC_PARTITION = 2*60*1000;

  static int DEF_BLOOM_EXPECTED_TUPLES = 10000;
  static double DEF_BLOOM_FALSE_POS_PROB = 0.01;

  /**
   * The input port on which events are received.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public final void process(INPUT tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * The output port on which deduped events are emitted.
   */
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();
  /**
   * The output port on which duplicate events are emitted.
   */
  public final transient DefaultOutputPort<OUTPUT> duplicates = new DefaultOutputPort<OUTPUT>();
  /**
   * The output port on which expired events are emitted.
   */
  public final transient DefaultOutputPort<OUTPUT> expired = new DefaultOutputPort<OUTPUT>();
  /**
   * The output port on which error events are emitted.
   */
  public final transient DefaultOutputPort<OUTPUT> error = new DefaultOutputPort<OUTPUT>();

  //Check-pointed state
  @NotNull
  protected BucketManager<INPUT> bucketManager;
  //bucketKey -> list of bucketData which belong to that bucket and are waiting for the bucket to be loaded.
  @NotNull
  protected final Map<Long, List<INPUT>> waitingEvents;
  protected Set<Integer> partitionKeys;
  protected int partitionMask;

  // Deduper Metrics
  @AutoMetric
  protected long Unique_Events;
  @AutoMetric
  protected long Duplicate_Events;
  @AutoMetric
  protected long Expired_Events;
  @AutoMetric
  protected long Error_Events;

  @Min(1)
  protected int partitionCount = 1;
  long start;
  protected boolean saveDataAtCheckpoint = false;
  protected long appWindow = 0;
  protected boolean orderedOutput = false;
  protected boolean enableDynamicPartitioning = true;
  protected long maxLatencyToIncreasePartition = DEFAULT_MAX_THRESH_DYNAMIC_PARTITION;
  protected long minLatencyToDecreasePartition = DEFAULT_MIN_THRESH_DYNAMIC_PARTITION;
  protected long millisBeforeNextPartition = DEFAULT_MILLIS_BEFORE_NEXT_DYNAMIC_PARTITION;

  //Non check-pointed state
  protected transient final BlockingQueue<AbstractBucket<INPUT>> fetchedBuckets;
  private transient long sleepTimeMillis;
  private transient OperatorContext context;
  private transient long currentWindow;
  private transient Map<INPUT, Decision> decisions;
  protected long lastPartitiondAt = System.currentTimeMillis();

  // Bloom filter configuration
  protected boolean isUseBloomFilter = true;
  protected transient Map<Long, BloomFilter<Object>> bloomFilters;
  protected int expectedNumTuples = DEF_BLOOM_EXPECTED_TUPLES;
  protected double falsePositiveProb = DEF_BLOOM_FALSE_POS_PROB;

  public AbstractDeduper()
  {
    waitingEvents = Maps.newHashMap();
    partitionKeys = Sets.newHashSet(0);
    partitionMask = 0;
    fetchedBuckets = new LinkedBlockingQueue<AbstractBucket<INPUT>>();
  }

  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;
    this.currentWindow = context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);
    sleepTimeMillis = context.getValue(OperatorContext.SPIN_MILLIS);
    lastPartitiondAt = System.currentTimeMillis();

    bucketManager.startService(this);
    for (long bucketKey : waitingEvents.keySet()) {
      bucketManager.loadBucketData(bucketKey);
    }
    if(orderedOutput)
    {
      decisions = Maps.newLinkedHashMap();
    }

    if(isUseBloomFilter){
      logger.debug("Bloom Filter Properties: Expected tuples {}, False Positive Prob {}", expectedNumTuples, falsePositiveProb);
      bloomFilters = Maps.newHashMap();
    }
  }

  @Override
  public void teardown()
  {
    bucketManager.shutdownService();
  }

  @Override
  public void beginWindow(long l)
  {
    currentWindow = l;

    // Reset Dedup Metrics
    Unique_Events = 0;
    Duplicate_Events = 0;
    Expired_Events = 0;

    // Reset Bucket Metrics
    ((AbstractBucketManager<?>)bucketManager).setBuckets_In_Memory(0);
    ((AbstractBucketManager<?>)bucketManager).setDeleted_Buckets(0);
    ((AbstractBucketManager<?>)bucketManager).setEvents_In_Memory(0);
    ((AbstractBucketManager<?>)bucketManager).setEvicted_Buckets(0);
    ((AbstractBucketManager<?>)bucketManager).setEvents_Committed_Last_Window(0);
    ((AbstractBucketManager<?>)bucketManager).setStart_Of_Buckets(0);
    ((AbstractBucketManager<?>)bucketManager).setEnd_Of_Buckets(0);

    start = System.currentTimeMillis();
    logger.debug("WINDOW {}",l);
  }

  /**
   * Checks whether an input tuple is qualified to be processed.
   *
   * @param tuple
   * @return
   */
  protected boolean isExpired(INPUT tuple)
  {
    long bucketKey = bucketManager.getBucketKeyFor(tuple);
    if (bucketKey < 0) {
      if(bucketKey == -1){
        if(orderedOutput && ! decisions.isEmpty()){
          recordDecision(tuple, Decision.EXPIRED, null);
        }
        else{
          processExpired(tuple);
        }
      }
      else if(bucketKey == -2) {
        if(orderedOutput && ! decisions.isEmpty()){
          recordDecision(tuple, Decision.ERROR, null);
        }
        else{
          processError(tuple);
        }
      }
      return true;
    } //ignore event
    return false;
  }

  protected void processTuple(INPUT tuple)
  {
    if(isExpired(tuple)){
      return;
    }
    long bucketKey = bucketManager.getBucketKeyFor(tuple);
    AbstractBucket<INPUT> bucket = bucketManager.getBucket(bucketKey);
    processNonExpiredTuple(tuple, bucket, bucketKey);
  }

  // This method can be overriden in implementation of Deduper.
  protected void processNonExpiredTuple(INPUT tuple, AbstractBucket<INPUT> bucket, long bucketKey)
  {
//      logger.debug("Processing: "+tuple);
        if(isUseBloomFilter && !waitingEvents.containsKey(bucketKey)) {
          Object tupleKey = getEventKey(tuple);
    
          if(bloomFilters.containsKey(bucketKey)){
            if( ! bloomFilters.get(bucketKey).contains(tupleKey)){
              bloomFilters.get(bucketKey).add(tupleKey); // Add tuple key to Bloom filter
    
              bucketManager.newEvent(bucketKey, tuple);
              processUnique(tuple, bucket);
              return;
            }
          }
        }

      if (bucket != null && !waitingEvents.containsKey(bucketKey) && bucket.isDataOnDiskLoaded() && bucket.containsEvent(tuple)) {
        if(orderedOutput && ! decisions.isEmpty()){
          recordDecision(tuple, Decision.DUPLICATE, bucket);
        }
        else{
          processDuplicate(tuple, bucket);
        }
        return;
      } //duplicate event

      if (bucket != null && !waitingEvents.containsKey(bucketKey) && bucket.isDataOnDiskLoaded()) {
        bucketManager.newEvent(bucketKey, tuple);
        if(orderedOutput && ! decisions.isEmpty()){
          recordDecision(tuple, Decision.UNIQUE, bucket);
        }
        else{
          processUnique(tuple, bucket);
        }
      }
      else {
        /**
         * The bucket on disk is not loaded. So we load the bucket from the disk.
         * Before that we check if there is a pending request to load the bucket and in that case we
         * put the event in a waiting list.
         */
        boolean doLoadFromDisk = false;
        List<INPUT> waitingList = waitingEvents.get(bucketKey);
        if (waitingList == null) {
          waitingList = Lists.newArrayList();
          waitingEvents.put(bucketKey, waitingList);
          doLoadFromDisk = true;
        }
        waitingList.add(tuple);
        if(orderedOutput){
          recordDecision(tuple, Decision.UNKNOWN, null);
        }

        if (doLoadFromDisk) {
          //Trigger the storage manager to load bucketData for this bucket key. This is a non-blocking call.
          bucketManager.loadBucketData(bucketKey);
        }
      }
  }

  protected void processUnique(INPUT tuple, AbstractBucket<INPUT> bucket)
  {
    if(isUseBloomFilter && bucket != null) {
      // Add event to bloom filter
      if( ! bloomFilters.containsKey(bucket.bucketKey)){
        bloomFilters.put(bucket.bucketKey, new BloomFilter<Object>(expectedNumTuples, falsePositiveProb));
      }
      bloomFilters.get(bucket.bucketKey).add(getEventKey(tuple));
    }

    Unique_Events++;
    output.emit(convert(tuple));
  }

  protected void processDuplicate(INPUT tuple, AbstractBucket<INPUT> bucket)
  {
    Duplicate_Events++;
    duplicates.emit(convert(tuple));
  }

  protected void processExpired(INPUT tuple)
  {
    Expired_Events++;
    expired.emit(convert(tuple));
  }

  protected void processError(INPUT tuple)
  {
    Error_Events++;
    error.emit(convert(tuple));
  }

  protected void recordDecision(INPUT tuple, Decision d, AbstractBucket<INPUT> bucket)
  {
    if(isUseBloomFilter && d == Decision.UNIQUE && bucket != null){
      if( ! bloomFilters.containsKey(bucket.bucketKey)){
        bloomFilters.put(bucket.bucketKey, new BloomFilter<Object>(expectedNumTuples, falsePositiveProb));
      }
      bloomFilters.get(bucket.bucketKey).add(getEventKey(tuple));
    }

    decisions.put(tuple, d);
  }

  @Override
  public void endWindow()
  {
    try {
      bucketManager.blockUntilAllRequestsServiced();
      handleIdleTime();
      Preconditions.checkArgument(waitingEvents.isEmpty(), waitingEvents.keySet());
      if(orderedOutput){
        emitProcessedTuples();
        Preconditions.checkArgument(decisions.isEmpty(), "events pending - "+decisions.size());
      }
      appWindow++;
      if(! saveDataAtCheckpoint)
      {
        bucketManager.endWindow(currentWindow);
      }
      else{ //FIXME: Hack for processing just before checkpoint.
        if(appWindow%context.getValue(DAGContext.CHECKPOINT_WINDOW_COUNT) == context.getValue(DAGContext.CHECKPOINT_WINDOW_COUNT)-1){
          bucketManager.endWindow(currentWindow);
          logger.debug("saving at checkpoint");
        }
      }
    }
    catch (Throwable cause) {
      DTThrowable.rethrow(cause);
    }
    logger.debug("time for window {}", System.currentTimeMillis() - start);
  }

  @Override
  public void checkpointed(long windowId)
  {
    logger.debug("Checkpointed Deduper");
    bucketManager.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
  }

  protected void emitProcessedTuples(){
    Iterator<Entry<INPUT, Decision>> entries = decisions.entrySet().iterator();
    while(entries.hasNext()){
      Entry<INPUT, Decision> td = entries.next();
      switch(td.getValue()){
      case UNIQUE:
        processUnique(td.getKey(), null);
        entries.remove();
        break;
      case DUPLICATE:
        processDuplicate(td.getKey(), null);
        entries.remove();
        break;
      case EXPIRED:
        processExpired(td.getKey());
        entries.remove();
      default:
        break;
      }
    }
  }

  @Override
  public void handleIdleTime()
  {
    if(orderedOutput){
      emitProcessedTuples();
    }
    if (fetchedBuckets.isEmpty()) {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    else {
      /**
       * Remove all the events from waiting list whose buckets are loaded.
       * Process these events again.
       */
      AbstractBucket<INPUT> bucket;
      while ((bucket = fetchedBuckets.poll()) != null) {
        List<INPUT> waitingList = waitingEvents.remove(bucket.bucketKey);
        if (waitingList != null) {
          for (INPUT event : waitingList) {
            if (!bucket.containsEvent(event)) {
              if(bucketManager.getBucketKeyFor(event) < 0){ // This event will be expired after all tuples in this window are finished processing.
                bucketManager.addEventToBucket(bucket, event); // Temporarily add the event to this bucket, so as to deduplicate within this window.
              }
              else{
                bucketManager.newEvent(bucket.bucketKey, event);
              }
              if(orderedOutput && ! decisions.isEmpty()){
                recordDecision(event, Decision.UNIQUE, bucket);
              }
              else{
                processUnique(event, bucket);
              }
            }
            else {
              if(orderedOutput && ! decisions.isEmpty()){
                recordDecision(event, Decision.DUPLICATE, bucket);
              }
              else{
                processDuplicate(event, bucket);
              }
            }
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void bucketLoaded(AbstractBucket<INPUT> loadedBucket)
  {
    if(isUseBloomFilter){
      // Load bloom filter for this bucket
      Set<Object> keys = loadedBucket.getWrittenEventKeys();
      if(keys != null) {
        if( ! bloomFilters.containsKey(loadedBucket.bucketKey)){
          bloomFilters.put(loadedBucket.bucketKey, new BloomFilter<Object>(expectedNumTuples, falsePositiveProb));
        }
        BloomFilter<Object> bf = bloomFilters.get(loadedBucket.bucketKey);
        for(Object key: keys) {
          bf.add(key);
        }
      }
    }

    fetchedBuckets.add(loadedBucket);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void bucketOffLoaded(long bucketKey)
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void bucketDeleted(long bucketKey)
  {
    if(isUseBloomFilter && bloomFilters != null && bloomFilters.containsKey(bucketKey)){
      // Remove bloom filter for this bucket and all previous buckets
      Iterator<Map.Entry<Long, BloomFilter<Object>>> it = bloomFilters.entrySet().iterator();
      while(it.hasNext()){
        long key = it.next().getKey();
        if(key <= bucketKey){
          it.remove();
        }
      }
    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractDeduper<INPUT, OUTPUT>>> partitions)
  {
  }

  @Override
  @SuppressWarnings({"deprecation"})
  public Collection<Partition<AbstractDeduper<INPUT, OUTPUT>>> definePartitions(Collection<Partition<AbstractDeduper<INPUT, OUTPUT>>> partitions, PartitioningContext context)
  {
    final int finalCapacity = DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);

    //Collect the state here
    Map<Integer, BucketManager<INPUT>> partitionKeyToOldManagers = Maps.newHashMap();
    List<BucketManager<INPUT>> oldStorageManagers = Lists.newArrayList();

    Map<Long, List<INPUT>> allWaitingEvents = Maps.newHashMap();

    int oldMask = 1;
    if(partitions.iterator().next().getPartitionKeys() != null && partitions.iterator().next().getPartitionKeys().get(input) != null) {
      oldMask = partitions.iterator().next().getPartitionKeys().get(input).mask;
    }

    for (Partition<AbstractDeduper<INPUT, OUTPUT>> partition : partitions) {
      //collect all bucketStorageManagers
      oldStorageManagers.add(partition.getPartitionedInstance().bucketManager);
      if(partition.getPartitionKeys().get(input) != null) {
        partitionKeyToOldManagers.put(partition.getPartitionKeys().get(input).partitions.iterator().next(), partition.getPartitionedInstance().bucketManager);
      }

      //collect all waiting events
//      for (Map.Entry<Long, List<INPUT>> awaitingList : partition.getPartitionedInstance().waitingEvents.entrySet()) {
//        if (awaitingList.getValue().size() > 0) {
//          List<INPUT> existingList = allWaitingEvents.get(awaitingList.getKey());
//          if (existingList == null) {
//            existingList = Lists.newArrayList();
//            allWaitingEvents.put(awaitingList.getKey(), existingList);
//          }
//          existingList.addAll(awaitingList.getValue());
//        }
//      }
//      partition.getPartitionedInstance().waitingEvents.clear();
    }

    partitions.clear();

    Collection<Partition<AbstractDeduper<INPUT, OUTPUT>>> newPartitions = Lists.newArrayListWithCapacity(finalCapacity);
    Map<Integer, BucketManager<INPUT>> partitionKeyToStorageManagers = Maps.newHashMap();

    // Create new instances using kryo
    for (int i = 0; i < finalCapacity; i++) {
      try {
        Kryo kryo = new Kryo();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, this);
        output.close();
        Input lInput = new Input(bos.toByteArray());

        @SuppressWarnings("unchecked")
        AbstractDeduper<INPUT, OUTPUT> deduper = (AbstractDeduper<INPUT, OUTPUT>) kryo.readObject(lInput, this.getClass());
        DefaultPartition<AbstractDeduper<INPUT, OUTPUT>> partition = new DefaultPartition<AbstractDeduper<INPUT, OUTPUT>>(deduper);
        newPartitions.add(partition);
      }
      catch (Throwable cause) {
        DTThrowable.rethrow(cause);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), input);
    int lPartitionMask = newPartitions.iterator().next().getPartitionKeys().get(input).mask;

    //transfer the state here
    for (Partition<AbstractDeduper<INPUT, OUTPUT>> deduperPartition : newPartitions) {
      AbstractDeduper<INPUT, OUTPUT> deduperInstance = deduperPartition.getPartitionedInstance();
      deduperInstance.partitionKeys = deduperPartition.getPartitionKeys().get(input).partitions;
      deduperInstance.partitionMask = lPartitionMask;
      logger.debug("partitions {},{}", deduperInstance.partitionKeys, deduperInstance.partitionMask);
      try {
        deduperInstance.bucketManager = bucketManager.clone();
        logger.debug("New Partition HdfsStore - {}",((HdfsBucketStore<INPUT>)((AbstractBucketManager<INPUT>)deduperInstance.bucketManager).getBucketStore()).toString());
      }
      catch (CloneNotSupportedException ex) {
        if ((deduperInstance.bucketManager = bucketManager.cloneWithProperties()) == null) {
          DTThrowable.rethrow(ex);
        }
        else {
          logger.warn("Please use clone method of bucketManager instead of cloneWithProperties");
        }
      }

      for (int partitionKey : deduperInstance.partitionKeys) {
        partitionKeyToStorageManagers.put(partitionKey, deduperInstance.bucketManager);
      }

      //distribute waiting events
//      for (long bucketKey : allWaitingEvents.keySet()) {
//        for (Iterator<INPUT> iterator = allWaitingEvents.get(bucketKey).iterator(); iterator.hasNext();) {
//          INPUT event = iterator.next();
//          int partitionKey = getEventKey(event).hashCode() & lPartitionMask;
//
//          if (deduperInstance.partitionKeys.contains(partitionKey)) {
//            List<INPUT> existingList = deduperInstance.waitingEvents.get(bucketKey);
//            if (existingList == null) {
//              existingList = Lists.newArrayList();
//              deduperInstance.waitingEvents.put(bucketKey, existingList);
//            }
//            existingList.add(event);
//            iterator.remove();
//          }
//        }
//      }
    }
    //let storage manager and subclasses distribute state as well
    bucketManager.definePartitions(partitionKeyToOldManagers, partitionKeyToStorageManagers, oldMask, lPartitionMask);
    logger.debug("new partitions - {}", newPartitions.size());
    return newPartitions;
  }

  /**
   * Sets the bucket manager.
   *
   * @param bucketManager {@link BucketManager} to be used by deduper.
   */
  public void setBucketManager(@NotNull BucketManager<INPUT> bucketManager)
  {
    this.bucketManager = Preconditions.checkNotNull(bucketManager, "storage manager");
  }

  public BucketManager<INPUT> getBucketManager()
  {
    return this.bucketManager;
  }

  /**
   * Converts the input tuple to output tuple.
   *
   * @param input input event.
   * @return output tuple derived from input.
   */
  protected abstract OUTPUT convert(INPUT input);
  protected abstract Object getEventKey(INPUT event);

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractDeduper)) {
      return false;
    }

    AbstractDeduper<?, ?> deduper = (AbstractDeduper<?, ?>) o;

    if (partitionMask != deduper.partitionMask) {
      return false;
    }
    if (!bucketManager.equals(deduper.bucketManager)) {
      return false;
    }
    if (partitionKeys != null ? !partitionKeys.equals(deduper.partitionKeys) : deduper.partitionKeys != null) {
      return false;
    }
    return waitingEvents.equals(deduper.waitingEvents);
  }

  @Override
  public int hashCode()
  {
    int result = bucketManager.hashCode();
    result = 31 * result + (waitingEvents.hashCode());
    result = 31 * result + (partitionKeys != null ? partitionKeys.hashCode() : 0);
    result = 31 * result + partitionMask;
    return result;
  }

  @Override
  public String toString()
  {
    return "Deduper{" + "partitionKeys=" + partitionKeys + ", partitionMask=" + partitionMask + '}';
  }

  public boolean isSaveDataAtCheckpoint()
  {
    return saveDataAtCheckpoint;
  }

  public void setSaveDataAtCheckpoint(boolean saveDataAtCheckpoint)
  {
    this.saveDataAtCheckpoint = saveDataAtCheckpoint;
  }

  public boolean isOrderedOutput()
  {
    return orderedOutput;
  }

  public void setOrderedOutput(boolean orderedOutput)
  {
    this.orderedOutput = orderedOutput;
  }

  public long getCurrentWindow()
  {
    return currentWindow;
  }

  public long getMaxLatencyToIncreasePartition()
  {
    return maxLatencyToIncreasePartition;
  }

  public void setMaxLatencyToIncreasePartition(long maxLatencyToIncreasePartition)
  {
    this.maxLatencyToIncreasePartition = maxLatencyToIncreasePartition;
  }

  public long getMinLatencyToDecreasePartition()
  {
    return minLatencyToDecreasePartition;
  }

  public void setMinLatencyToDecreasePartition(long minLatencyToDecreasePartition)
  {
    this.minLatencyToDecreasePartition = minLatencyToDecreasePartition;
  }

  public long getMillisBeforeNextPartition()
  {
    return millisBeforeNextPartition;
  }

  public void setMillisBeforeNextPartition(long millisBeforeNextPartition)
  {
    this.millisBeforeNextPartition = millisBeforeNextPartition;
  }

  public boolean isEnableDynamicPartitioning()
  {
    return enableDynamicPartitioning;
  }

  public void setEnableDynamicPartitioning(boolean enableDynamicPartitioning)
  {
    this.enableDynamicPartitioning = enableDynamicPartitioning;
  }

  // Bucket Manager Metrics
  @AutoMetric
  public long get_Deleted_Buckets(){
    return ((AbstractBucketManager<?>)bucketManager).getDeleted_Buckets();
  }

  @AutoMetric
  public long get_Evicted_Buckets()
  {
    return ((AbstractBucketManager<?>)bucketManager).getEvicted_Buckets();
  }

  @AutoMetric
  public long get_Events_In_Memory()
  {
    return ((AbstractBucketManager<?>)bucketManager).getEvents_In_Memory();
  }

  @AutoMetric
  public long get_Events_Committed_Last_Window()
  {
    return ((AbstractBucketManager<?>)bucketManager).getEvents_Committed_Last_Window();
  }

  @AutoMetric
  public long get_End_Of_Buckets()
  {
    return ((AbstractBucketManager<?>)bucketManager).getEnd_Of_Buckets();
  }

  @AutoMetric
  public long get_Start_Of_Buckets()
  {
    return ((AbstractBucketManager<?>)bucketManager).getStart_Of_Buckets();
  }

  public void setUseBloomFilter(boolean isUseBloomFilter)
  {
    this.isUseBloomFilter = isUseBloomFilter;
  }

  public void setExpectedNumTuples(int expectedNumTuples)
  {
    this.expectedNumTuples = expectedNumTuples;
  }

  public void setFalsePositiveProb(double falsePositiveProb)
  {
    this.falsePositiveProb = falsePositiveProb;
  }

  public boolean isUseBloomFilter()
  {
    return isUseBloomFilter;
  }

  public int getExpectedNumTuples()
  {
    return expectedNumTuples;
  }

  public double getFalsePositiveProb()
  {
    return falsePositiveProb;
  }


  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response response = null;
    if(!enableDynamicPartitioning)
    {
      return null;
    }
    if(System.currentTimeMillis() - lastPartitiondAt < millisBeforeNextPartition)
    {
      return null;
    }
    if(batchedOperatorStats.getLatencyMA() > maxLatencyToIncreasePartition)
    {
      logger.debug("Latency above threshold {}", maxLatencyToIncreasePartition);
      response = new Response();
      response.repartitionRequired = true;
      partitionCount *= 2;
      lastPartitiondAt = System.currentTimeMillis();
    }
    else if(batchedOperatorStats.getLatencyMA() < minLatencyToDecreasePartition)
    {
      logger.debug("Latency below threshold {}", minLatencyToDecreasePartition);
      response = new Response();
      if(partitionCount / 2 >= 1)
      {
        response.repartitionRequired = true;
        partitionCount /= 2;
        lastPartitiondAt = System.currentTimeMillis();
      }
    }
    return response;
  }

  protected enum Decision{
    UNIQUE,
    DUPLICATE,
    EXPIRED,
    ERROR,
    UNKNOWN
  }

  private final static Logger logger = LoggerFactory.getLogger(AbstractDeduper.class);
}