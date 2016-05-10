/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.netlet.util.DTThrowable;

public class DeduperAutoPartitioner<INPUT, OUTPUT> implements Partitioner<AbstractDeduper<INPUT,OUTPUT>>
{

  @Min(1)
  private int partitionCount = 1;

  @Override
  public Collection<Partition<AbstractDeduper<INPUT, OUTPUT>>> definePartitions(
      Collection<Partition<AbstractDeduper<INPUT, OUTPUT>>> partitions, PartitioningContext context)
  {
    final int finalCapacity = DefaultPartition.getRequiredPartitionCount(context, partitionCount);

    //Collect the state here
    List<BucketManager<INPUT>> oldStorageManagers = Lists.newArrayList();

    Map<Long, List<INPUT>> allWaitingEvents = Maps.newHashMap();

    for (Partition<AbstractDeduper<INPUT, OUTPUT>> partition : partitions) {
      //collect all bucketStorageManagers
      oldStorageManagers.add(partition.getPartitionedInstance().bucketManager);

      //collect all waiting events
      for (Map.Entry<Long, List<INPUT>> awaitingList : partition.getPartitionedInstance().waitingEvents.entrySet()) {
        if (awaitingList.getValue().size() > 0) {
          List<INPUT> existingList = allWaitingEvents.get(awaitingList.getKey());
          if (existingList == null) {
            existingList = Lists.newArrayList();
            allWaitingEvents.put(awaitingList.getKey(), existingList);
          }
          existingList.addAll(awaitingList.getValue());
        }
      }
      partition.getPartitionedInstance().waitingEvents.clear();
    }

    BucketManager<INPUT> bucketManager;
    try {
      bucketManager = oldStorageManagers.iterator().next().clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    partitions.clear();

    Collection<Partition<AbstractDeduper<INPUT, OUTPUT>>> newPartitions = Lists.newArrayListWithCapacity(finalCapacity);
    Map<Integer, BucketManager<INPUT>> partitionKeyToStorageManagers = Maps.newHashMap();

    for (int i = 0; i < finalCapacity; i++) {
      try {
        Kryo kryo = new Kryo();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, this);
        output.close();
        Input lInput = new Input(bos.toByteArray());

        @SuppressWarnings("unchecked")
        AbstractDeduper<INPUT, OUTPUT> deduper = (AbstractDeduper<INPUT, OUTPUT>)kryo.readObject(lInput,
            AbstractDeduper.class);
        DefaultPartition<AbstractDeduper<INPUT, OUTPUT>> partition = new DefaultPartition<>(deduper);
        newPartitions.add(partition);
      } catch (Throwable cause) {
        DTThrowable.rethrow(cause);
      }
    }

    List<InputPort<?>> inputPortList = context.getInputPorts();
    InputPort<?> inputPort = inputPortList.iterator().next();
    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), inputPort);
    int lPartitionMask = newPartitions.iterator().next().getPartitionKeys().get(inputPort).mask;

    //transfer the state here
    for (Partition<AbstractDeduper<INPUT, OUTPUT>> deduperPartition : newPartitions) {
      AbstractDeduper<INPUT, OUTPUT> deduperInstance = deduperPartition.getPartitionedInstance();

      deduperInstance.partitionKeys = deduperPartition.getPartitionKeys().get(inputPort).partitions;
      deduperInstance.partitionMask = lPartitionMask;
      logger.debug("partitions {},{}", deduperInstance.partitionKeys, deduperInstance.partitionMask);
      try {
        deduperInstance.bucketManager = bucketManager.clone();
      } catch (CloneNotSupportedException ex) {
        if ((deduperInstance.bucketManager = bucketManager.cloneWithProperties()) == null) {
          DTThrowable.rethrow(ex);
        } else {
          logger.warn("Please use clone method of bucketManager instead of cloneWithProperties");
        }
      }

      for (int partitionKey : deduperInstance.partitionKeys) {
        partitionKeyToStorageManagers.put(partitionKey, deduperInstance.bucketManager);
      }

      //distribute waiting events
      for (long bucketKey : allWaitingEvents.keySet()) {
        for (Iterator<INPUT> iterator = allWaitingEvents.get(bucketKey).iterator(); iterator.hasNext();) {
          INPUT event = iterator.next();
          int partitionKey = deduperInstance.getEventKey(event).hashCode() & lPartitionMask;

          if (deduperInstance.partitionKeys.contains(partitionKey)) {
            List<INPUT> existingList = deduperInstance.waitingEvents.get(bucketKey);
            if (existingList == null) {
              existingList = Lists.newArrayList();
              deduperInstance.waitingEvents.put(bucketKey, existingList);
            }
            existingList.add(event);
            iterator.remove();
          }
        }
      }
    }
    //let storage manager and subclasses distribute state as well
    bucketManager.definePartitions(oldStorageManagers, partitionKeyToStorageManagers, lPartitionMask);
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractDeduper<INPUT, OUTPUT>>> partitions)
  {

  }

  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  private static final Logger logger = LoggerFactory.getLogger(DeduperAutoPartitioner.class);
}
