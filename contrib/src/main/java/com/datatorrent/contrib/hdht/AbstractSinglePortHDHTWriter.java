/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hdht;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.hdht.HDHTWalManager.PreviousWALDetails;
import com.datatorrent.contrib.hdht.HDHTWalManager.WalPosition;
import com.datatorrent.netlet.util.Slice;

/**
 * Operator that receives data on port and writes it to the data store.
 * Implements partitioning, maps partition key to the store bucket.
 * The derived class supplies the codec for partitioning and key-value serialization.
 * @param <EVENT>
 *
 * @since 2.0.0
 */
public abstract class AbstractSinglePortHDHTWriter<EVENT> extends HDHTWriter implements
    Partitioner<AbstractSinglePortHDHTWriter<EVENT>>, StatsListener
{
  public interface HDHTCodec<EVENT> extends StreamCodec<EVENT>
  {
    byte[] getKeyBytes(EVENT event);

    byte[] getValueBytes(EVENT event);

    EVENT fromKeyValue(Slice key, byte[] value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSinglePortHDHTWriter.class);

  protected int partitionMask;

  protected Set<Integer> partitions;

  protected transient HDHTCodec<EVENT> codec;

  @Min(1)
  private int partitionCount = 1;

  private int currentPartitions;

  /**
   * set default to 1 as a valid value and also align with partitionCount.
   * throw error if client code set its value to 0
   */
  @Min(1)
  private int numberOfBuckets = 1;

  /**
   * Indicates if number of buckets have been finalized to ensure the bucket count is not changed dynamically.
   */
  private boolean numberOfBucketsFinalized = false;

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<EVENT> input = new DefaultInputPort<EVENT>()
  {
    @Override
    public void process(EVENT event)
    {
      try {
        processEvent(event);
      } catch (IOException e) {
        throw new RuntimeException("Error processing " + event, e);
      }
    }

    @Override
    public StreamCodec<EVENT> getStreamCodec()
    {
      return getCodec();
    }
  };

  public void setPartitionCount(int partitionCount)
  {
    if (!numberOfBucketsFinalized && this.partitionCount > this.numberOfBuckets) {
      this.setNumberOfBuckets(partitionCount);
    }
    this.partitionCount = partitionCount;
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * Storage bucket for the given event. Only one partition can write to a storage bucket and by default it is
   * identified by the partition id.
   *
   * @param event
   * @return The bucket key.
   */
  protected long getBucketKey(EVENT event)
  {
    return (codec.getPartition(event) & partitionMask);
  }

  protected void processEvent(EVENT event) throws IOException
  {
    byte[] key = codec.getKeyBytes(event);
    byte[] value = codec.getValueBytes(event);
    super.put(getBucketKey(event), new Slice(key), value);
  }

  protected abstract HDHTCodec<EVENT> getCodec();

  @Override
  public void setup(OperatorContext arg0)
  {
    LOG.debug("Store {} with partitions {} {}", super.getFileStore(),
        new PartitionKeys(this.partitionMask, this.partitions));

    if (!numberOfBucketsFinalized) {
      // In case of single partition, definePartitions won't be invoked. So, explicitly set partition mask and bucket keys managed
      this.partitionMask = setPartitionMask();
      this.numberOfBuckets = this.partitionMask + 1;
      for (long i = 0; i < this.numberOfBuckets; i++) {
        this.bucketKeys.add(i);
      }
      // This is to ensure that number of buckets are not changed dynamically
      numberOfBucketsFinalized = true;
    }

    super.setup(arg0);
    try {
      this.codec = getCodec();
      // inject the operator reference, if such field exists
      // TODO: replace with broader solution
      Class<?> cls = this.codec.getClass();
      while (cls != null) {
        for (Field field : cls.getDeclaredFields()) {
          if (field.getType().isAssignableFrom(this.getClass())) {
            field.setAccessible(true);
            field.set(this.codec, this);
          }
        }
        cls = cls.getSuperclass();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create codec", e);
    }
  }

  /**
   * Repartition is required when number of partitions are not equal to required
   * partitions.
   * @param batchedOperatorStats the stats to use when repartitioning.
   * @return Returns the stats listener response.
   */
  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response res = new Response();
    res.repartitionRequired = false;
    if (currentPartitions != partitionCount) {
      LOG.info("processStats: trying repartition of input operator current {} required {}", currentPartitions, partitionCount);
      res.repartitionRequired = true;
    }
    return res;
  }
    
  @Override
  public Collection<Partition<AbstractSinglePortHDHTWriter<EVENT>>> definePartitions(
      Collection<Partition<AbstractSinglePortHDHTWriter<EVENT>>> partitions, PartitioningContext context)
  {
    final int newPartitionCount = DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);
    if (this.numberOfBuckets < newPartitionCount) {
      LOG.warn("Number of partitions should be less than number of buckets");
      this.partitionCount = partitions.size();
      return partitions;
    }
    LOG.info("In define partitions... new partition count = {}, Original partitions = {}, {}", newPartitionCount, partitions.isEmpty() ? 0 : partitions.size(), currentPartitions);
    if (newPartitionCount == partitions.size()) {
      return partitions;
    }

    Map<Long, PreviousWALDetails> originalBucketKeyToPartitionMap = Maps.newHashMap();
    Map<Long, Set<PreviousWALDetails>> staleWalFilesForPartitionMap = Maps.newHashMap();
    // Distribute original state to new Partitions
    // Collect WAL data for buckets and create Parent WAL details to associate with buckets on redistribution
    for (Partition<AbstractSinglePortHDHTWriter<EVENT>> partition : partitions) {
      AbstractSinglePortHDHTWriter<EVENT> oper = partition.getPartitionedInstance();
      // Minimum bucket start recovery position
      WalPosition startPosition = oper.minimumRecoveryWalPosition;
      PreviousWALDetails walDetails = new PreviousWALDetails(oper.getWalKey(), startPosition, oper.singleWalMeta.cpWalPosition, 
          oper.walPositions, oper.committedWalPosition, oper.singleWalMeta.windowId);
      LOG.info("Original Bucket keys = {}", oper.bucketKeys);
      for (Long bucketKey : oper.bucketKeys) {
        if (!oper.parentWals.isEmpty()) {
          LOG.info("Parent WAL is not empty for operator id = {} WAL list = {}", oper.getWalKey(), oper.parentWals);
          walDetails = oper.parentWalMetaDataMap.get(bucketKey);
        }
        if (startPosition != null) {
          originalBucketKeyToPartitionMap.put(bucketKey, walDetails);
        }
        staleWalFilesForPartitionMap.put(bucketKey, oper.alreadyCopiedWals);
      }
    }

    Kryo lKryo = new Kryo();
    Collection<Partition<AbstractSinglePortHDHTWriter<EVENT>>> newPartitions =
        Lists.newArrayListWithExpectedSize(newPartitionCount);
    for (int i = 0; i < newPartitionCount; i++) {
      // Kryo.copy fails as it attempts to clone transient fields (input port)
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      lKryo.writeObject(output, this);
      output.close();
      Input lInput = new Input(bos.toByteArray());
      @SuppressWarnings("unchecked")
      AbstractSinglePortHDHTWriter<EVENT> oper = lKryo.readObject(lInput, this.getClass());
      newPartitions.add(new DefaultPartition<AbstractSinglePortHDHTWriter<EVENT>>(oper));
    }

    // assign the partition keys
    assignPartitionKeys(newPartitions, input);

    for (Partition<AbstractSinglePortHDHTWriter<EVENT>> p : newPartitions) {
      PartitionKeys pks = p.getPartitionKeys().get(input);
      p.getPartitionedInstance().partitionMask = pks.mask;
      p.getPartitionedInstance().partitions = pks.partitions;
      p.getPartitionedInstance().bucketKeys.clear();
      for (Integer bucketKey : pks.partitions) {
        p.getPartitionedInstance().bucketKeys.add(new Long(bucketKey));
      }
      LOG.info("Repartitioned Bucket keys = {}", p.getPartitionedInstance().bucketKeys);
      if (!numberOfBucketsFinalized) {
        LOG.info("Number of buckets = {} Partition mask = {}", p.getPartitionedInstance().numberOfBuckets, p.getPartitionedInstance().partitionMask);
        p.getPartitionedInstance().numberOfBuckets = p.getPartitionedInstance().partitionMask + 1; // E.g. for 8 buckets mask is 0x111
        p.getPartitionedInstance().numberOfBucketsFinalized = true;
      }
      // Assign previous WAL details for each partition based on the buckets being managed by the partition. 
      if (!originalBucketKeyToPartitionMap.isEmpty()) {
        for (Long bucketKey : p.getPartitionedInstance().bucketKeys) {
          PreviousWALDetails previousWalDetails = originalBucketKeyToPartitionMap.get(bucketKey);
          if (previousWalDetails.needsRecovery()) {
            p.getPartitionedInstance().parentWalMetaDataMap = originalBucketKeyToPartitionMap;
            p.getPartitionedInstance().parentWals.add(previousWalDetails);
            p.getPartitionedInstance().alreadyCopiedWals.addAll(staleWalFilesForPartitionMap.get(bucketKey));
          }
        }
      }
    }

    return newPartitions;
  }

  public void assignPartitionKeys(Collection<Partition<AbstractSinglePortHDHTWriter<EVENT>>> partitions, InputPort<?> inputPort)
  {
    if (partitions.isEmpty()) {
      throw new IllegalArgumentException("partitions collection cannot be empty");
    }

    LOG.info("Total number of buckets = {}, Partitions = {}", numberOfBuckets, partitions.size());
    int partitionMask = setPartitionMask();

    Iterator<Partition<AbstractSinglePortHDHTWriter<EVENT>>> iterator = partitions.iterator();
    for (int i = 0; i <= partitionMask; i++) {
      Partition<?> p;
      if (iterator.hasNext()) {
        p = iterator.next();
      } else {
        iterator = partitions.iterator();
        p = iterator.next();
      }

      PartitionKeys pks = p.getPartitionKeys().get(inputPort);
      if (pks == null) {
        p.getPartitionKeys().put(inputPort, new PartitionKeys(partitionMask, Sets.newHashSet(i)));
      } else {
        pks.partitions.add(i);
      }
    }
  }

  private int setPartitionMask()
  {
    // Decide partition mask based on total buckets to support. 
    // Assign buckets in round robin way to partitions
    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(numberOfBuckets - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }
    return partitionMask;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractSinglePortHDHTWriter<EVENT>>> partitions)
  {
    LOG.info("Current partitions count = {}", currentPartitions);
    currentPartitions = partitions.size();
  }

  public int getNumberOfBuckets()
  {
    return numberOfBuckets;
  }

  /**
   * Sets total number of buckets to create for HDHT
   * Operator allocates closest power of 2 number of buckets
   * @param number of buckets
   */
  public void setNumberOfBuckets(int numberOfBuckets)
  {
    if (!numberOfBucketsFinalized) {
      this.numberOfBuckets = numberOfBuckets;
    } else {
      LOG.warn("Number of buckets cannot be changed after HDHT operators are deployed");
    }
  }

  @VisibleForTesting
  public int getCurrentPartitions()
  {
    return this.currentPartitions;
  }
}
