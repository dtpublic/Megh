/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.kafka;

import java.util.Collection;

import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.lib.bandwidth.BandwidthLimitingOperator;
import com.datatorrent.lib.bandwidth.BandwidthManager;
import com.datatorrent.lib.bandwidth.BandwidthPartitioner;

/**
 * KafkaByteArrayInputOperator extends from KafkaSinglePortByteArrayInputOperator and
 * serves the functionality of BandwidthLimitingOperator.&nbsp;
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Have only one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName Kafka Byte Array Input
 * @category Messaging
 * @tags input operator
 *
 */
public class KafkaByteArrayInputOperator extends KafkaSinglePortByteArrayInputOperator
    implements BandwidthLimitingOperator

{
  private BandwidthManager bandwidthManager;

  @AutoMetric
  private long inputMessagesPerSec;

  @AutoMetric
  private long inputBytesPerSec;

  private long messageCount;
  private long byteCount;
  private double windowTimeSec;

  public KafkaByteArrayInputOperator()
  {
    bandwidthManager = new BandwidthManager();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    bandwidthManager.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    inputMessagesPerSec = 0;
    inputBytesPerSec = 0;
    messageCount = 0;
    byteCount = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    inputBytesPerSec = (long)(byteCount / windowTimeSec);
    inputMessagesPerSec = (long)(messageCount / windowTimeSec);
  }

  @Override
  public Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> definePartitions(Collection<Partition
      <AbstractKafkaInputOperator<KafkaConsumer>>> partitions, PartitioningContext context)
  {
    KafkaByteArrayInputOperator currentPartition = (KafkaByteArrayInputOperator)partitions.iterator().next()
        .getPartitionedInstance();
    long currentBandwidth = currentPartition.getBandwidthManager().getBandwidth() * partitions.size();
    Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> newPartitions =  super.definePartitions(partitions,
        context);
    new BandwidthPartitioner().updateBandwidth(newPartitions, currentBandwidth);
    return newPartitions;
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }
    int sendCount = consumer.messageSize();
    if (getMaxTuplesPerWindow() > 0) {
      sendCount = Math.min(sendCount, getMaxTuplesPerWindow() - (int)messageCount);
    }
    for (int i = 0; i < sendCount; i++) {
      if (!bandwidthManager.canConsumeBandwidth()) {
        break;
      }
      KafkaConsumer.KafkaMessage message = consumer.pollMessage();
      // Ignore the duplicate messages
      if (offsetStats.containsKey(message.getKafkaPart()) && message.getOffSet() <=
          offsetStats.get(message.getKafkaPart())) {
        continue;
      }
      emitTuple(message.getMsg());
      messageCount++;
      byteCount += message.getMsg().size();
      bandwidthManager.consumeBandwidth(message.getMsg().size());
      offsetStats.put(message.getKafkaPart(), message.getOffSet());
      MutablePair<Long, Integer> offsetAndCount = currentWindowRecoveryState.get(message.getKafkaPart());
      if (offsetAndCount == null) {
        currentWindowRecoveryState.put(message.getKafkaPart(),
            new MutablePair<Long, Integer>(message.getOffSet(), 1));
      } else {
        offsetAndCount.setRight(offsetAndCount.right + 1);
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    bandwidthManager.teardown();
  }

  @Override
  public BandwidthManager getBandwidthManager()
  {
    return bandwidthManager;
  }
}
