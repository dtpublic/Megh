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
package com.datatorrent.apps.ingestion.io.input;

import java.nio.ByteBuffer;
import java.util.Collection;

import kafka.message.Message;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.io.BandwidthLimitingOperator;
import com.datatorrent.apps.ingestion.lib.BandwidthManager;
import com.datatorrent.apps.ingestion.lib.BandwidthPartitioner;
import com.datatorrent.contrib.kafka.AbstractKafkaInputOperator;
import com.datatorrent.contrib.kafka.AbstractKafkaSinglePortInputOperator;
import com.datatorrent.contrib.kafka.KafkaConsumer;

  /**
   * <p>KafkaSinglePortByteArrayInputOperator class.</p>
   *
   * @since 2.1.0
   */
  public class KafkaSinglePortByteArrayInputOperator extends AbstractKafkaSinglePortInputOperator<byte[]> implements BandwidthLimitingOperator
  {
    private BandwidthManager bandwidthManager;

    public KafkaSinglePortByteArrayInputOperator()
    {
      bandwidthManager = new BandwidthManager();
    }

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      bandwidthManager.setup(context);
    }

    @Override
    public Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> definePartitions(Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> partitions, PartitioningContext context)
    {
      KafkaSinglePortByteArrayInputOperator currentPartition = (KafkaSinglePortByteArrayInputOperator) partitions.iterator().next().getPartitionedInstance();
      long currentBandwidth = currentPartition.getBandwidthManager().getBandwidth() * partitions.size();
      Collection<Partition<AbstractKafkaInputOperator<KafkaConsumer>>> newPartitions =  super.definePartitions(partitions, context);
      new BandwidthPartitioner().updateBandwidth(newPartitions, currentBandwidth);
      return newPartitions;
    }

    @Override
    public void emitTuple(Message msg)
    {
      if(bandwidthManager.canConsumeBandwidth())
      {
        super.emitTuple(msg);
        bandwidthManager.consumeBandwidth(msg.size());
      }
    }

    /**
     * Implement abstract method of AbstractKafkaSinglePortInputOperator
     *
     * @param message
     * @return byte Array
     */
    @Override
    public byte[] getTuple(Message message)
    {
      byte[] bytes = null;
      try {
        ByteBuffer buffer = message.payload();
        bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
      }
      catch (Exception ex) {
        return bytes;
      }
      return bytes;
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

    public void setBandwidthManager(BandwidthManager bandwidthManager)
    {
      this.bandwidthManager = bandwidthManager;
    }
  }
