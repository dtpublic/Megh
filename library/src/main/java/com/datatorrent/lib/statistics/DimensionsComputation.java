/**
 * Copyright (c) 2016 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.statistics;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent.Aggregator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

/**
 * <p>An implementation of an operator that computes dimensions of events. </p>
 * <p>
 * @displayName Dimension Computation
 * @category Stats and Aggregations
 * @tags event, dimension, aggregation, computation
 *
 * @param <EVENT> - Type of the tuple whose attributes are used to define dimensions.
 * @since 1.0.2
 */
@org.apache.hadoop.classification.InterfaceStability.Unstable
public class DimensionsComputation<EVENT, AGGREGATE extends AggregateEvent> implements Operator
{
  private Unifier<AGGREGATE> unifier;

  public void setUnifier(Unifier<AGGREGATE> unifier)
  {
    this.unifier = unifier;
  }

  /**
   * Output port that emits an aggregate of events.
   */
  public final transient DefaultOutputPort<AGGREGATE> output = new DefaultOutputPort<AGGREGATE>()
  {
    @Override
    public Unifier<AGGREGATE> getUnifier()
    {
      if (DimensionsComputation.this.unifier == null) {
        return super.getUnifier();
      } else {
        return unifier;
      }
    }
  };

  protected void processInputTuple(EVENT tuple)
  {
    for (int i = 0; i < incrementalAggregatorMaps.length; i++) {
      incrementalAggregatorMaps[i].add(tuple, i);
    }
  }

  /**
   * Input data port that takes an event.
   */
  public final transient DefaultInputPort<EVENT> data = new DefaultInputPort<EVENT>()
  {
    @Override
    public void process(EVENT tuple)
    {
      processInputTuple(tuple);
    }
  };
//////////
//  public static interface Aggregator<EVENT, AGGREGATE extends AggregateEvent> extends HashingStrategy<EVENT>
//  {
//    AGGREGATE getGroup(EVENT src, int aggregatorIndex);
//
//    void aggregate(AGGREGATE dest, EVENT src);
//
//    void aggregate(AGGREGATE dest, AGGREGATE src);
//  }

  /**
   * incrementalAggregatorMaps was initialized by setAggregators(Aggregator<EVENT, AGGREGATE>[]) which was called
   * by AbstractDimensionsComputationFlexibleSingleSchema.setup(OperatorContext).
   * So it is not necessary to keep the state
   */
  private transient AggregatorMap<EVENT, AGGREGATE>[] incrementalAggregatorMaps;
  
  /**
   * Set the dimensions which should each get the tuples going forward.
   * A dimension is specified by the colon separated list of fields names which together forms dimension.
   * Dimesions are separated by comma. This form is chosen so that dimensions can be controlled through
   * properties file as well.
   *
   * @param incrementalAggregators
   */
  public void setAggregators(Aggregator<EVENT, AGGREGATE>[] incrementalAggregators)
  {
    @SuppressWarnings("unchecked")
    AggregatorMap<EVENT, AGGREGATE>[] newInstance =
        (AggregatorMap<EVENT, AGGREGATE>[])Array.newInstance(AggregatorMap.class, incrementalAggregators.length);
    incrementalAggregatorMaps = newInstance;
    for (int i = incrementalAggregators.length; i-- > 0; ) {
      incrementalAggregatorMaps[i] = new AggregatorMap<EVENT, AGGREGATE>(incrementalAggregators[i]);
    }
  }

  public Aggregator<EVENT, AGGREGATE>[] getAggregators()
  {
    @SuppressWarnings("unchecked")
    Aggregator<EVENT, AGGREGATE>[] aggregators =
        (Aggregator<EVENT, AGGREGATE>[])Array.newInstance(Aggregator.class, incrementalAggregatorMaps.length);
    for (int i = incrementalAggregatorMaps.length; i-- > 0; ) {
      aggregators[i] = incrementalAggregatorMaps[i].aggregator;
    }
    return aggregators;
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    for (AggregatorMap<EVENT, AGGREGATE> dimension : incrementalAggregatorMaps) {
      for (AGGREGATE value : dimension.values()) {
        output.emit(value);
      }
      dimension.clear();
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public void transferDimension(Aggregator<EVENT, AGGREGATE> aggregator, DimensionsComputation<EVENT, AGGREGATE> other)
  {
    if (other.incrementalAggregatorMaps == null) {
      if (this.incrementalAggregatorMaps == null) {
        @SuppressWarnings("unchecked")
        AggregatorMap<EVENT, AGGREGATE>[] newInstance =
            (AggregatorMap<EVENT, AGGREGATE>[])Array.newInstance(AggregatorMap.class, 1);
        this.incrementalAggregatorMaps = newInstance;
      } else {
        this.incrementalAggregatorMaps = Arrays.copyOf(this.incrementalAggregatorMaps,
            this.incrementalAggregatorMaps.length + 1);
      }

      this.incrementalAggregatorMaps[this.incrementalAggregatorMaps.length - 1] =
          new AggregatorMap<EVENT, AGGREGATE>(aggregator);
    } else {
      int i = other.incrementalAggregatorMaps.length;
      while (i-- > 0) {
        AggregatorMap<EVENT, AGGREGATE> otherMap = other.incrementalAggregatorMaps[i];
        if (aggregator.equals(otherMap.aggregator)) {
          other.incrementalAggregatorMaps[i] = null;
          @SuppressWarnings("unchecked")
          AggregatorMap<EVENT, AGGREGATE>[] newArray =
              (AggregatorMap<EVENT, AGGREGATE>[])Array.newInstance(AggregatorMap.class,
                  other.incrementalAggregatorMaps.length - 1);

          i = 0;
          for (AggregatorMap<EVENT, AGGREGATE> dimesion : other.incrementalAggregatorMaps) {
            if (dimesion != null) {
              newArray[i++] = dimesion;
            }
          }
          other.incrementalAggregatorMaps = newArray;

          if (this.incrementalAggregatorMaps == null) {
            @SuppressWarnings("unchecked")
            AggregatorMap<EVENT, AGGREGATE>[] newInstance =
                (AggregatorMap<EVENT, AGGREGATE>[])Array.newInstance(AggregatorMap.class, 1);
            this.incrementalAggregatorMaps = newInstance;
          } else {
            this.incrementalAggregatorMaps = Arrays.copyOf(this.incrementalAggregatorMaps,
                this.incrementalAggregatorMaps.length + 1);
          }

          this.incrementalAggregatorMaps[this.incrementalAggregatorMaps.length - 1] = otherMap;
          break;
        }
      }
    }
  }

  public static class PartitionerImpl<EVENT,AGGREGATE extends AggregateEvent>
      implements Partitioner<DimensionsComputation<EVENT, AGGREGATE>>
  {
    @Min(1)
    private int partitionCount;

    public void setPartitionCount(int partitionCount)
    {
      this.partitionCount = partitionCount;
    }

    public int getPartitionCount()
    {
      return partitionCount;
    }

    @Override
    public void partitioned(Map<Integer, Partition<DimensionsComputation<EVENT, AGGREGATE>>> partitions)
    {
    }

    @Override
    public Collection<Partition<DimensionsComputation<EVENT, AGGREGATE>>> definePartitions(
        Collection<Partition<DimensionsComputation<EVENT, AGGREGATE>>> partitions, PartitioningContext context)
    {
      int newPartitionsCount = DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);

      LinkedHashMap<Aggregator<EVENT, AGGREGATE>, DimensionsComputation<EVENT, AGGREGATE>> map =
          new LinkedHashMap<Aggregator<EVENT, AGGREGATE>, DimensionsComputation<EVENT, AGGREGATE>>(newPartitionsCount);
      for (Partition<DimensionsComputation<EVENT, AGGREGATE>> partition : partitions) {
        for (Aggregator<EVENT, AGGREGATE> dimension : partition.getPartitionedInstance().getAggregators()) {
          map.put(dimension, partition.getPartitionedInstance());
        }
      }

      int remainingDimensions = map.size();
      if (newPartitionsCount > remainingDimensions) {
        if (context.getParallelPartitionCount() == 0) {
          newPartitionsCount = remainingDimensions;
        } else {
          logger.error("Cannot distribute {} dimensions over {} parallel partitions.",
              remainingDimensions, newPartitionsCount);
          return partitions;
        }
      }

      int[] dimensionsPerPartition = new int[newPartitionsCount];
      while (remainingDimensions > 0) {
        for (int i = 0; i < newPartitionsCount && remainingDimensions > 0; i++) {
          dimensionsPerPartition[i]++;
          remainingDimensions--;
        }
      }

      ArrayList<Partition<DimensionsComputation<EVENT, AGGREGATE>>> returnValue =
          new ArrayList<Partition<DimensionsComputation<EVENT, AGGREGATE>>>(newPartitionsCount);

      Iterator<Entry<Aggregator<EVENT, AGGREGATE>, DimensionsComputation<EVENT, AGGREGATE>>> iterator =
          map.entrySet().iterator();
      for (int i = 0; i < newPartitionsCount; i++) {
        DimensionsComputation<EVENT, AGGREGATE> dc = new DimensionsComputation<EVENT, AGGREGATE>();
        for (int j = 0; j < dimensionsPerPartition[i]; j++) {
          Entry<Aggregator<EVENT,AGGREGATE>, DimensionsComputation<EVENT, AGGREGATE>> next = iterator.next();
          dc.transferDimension(next.getKey(), next.getValue());
        }

        DefaultPartition<DimensionsComputation<EVENT, AGGREGATE>> partition =
            new DefaultPartition<DimensionsComputation<EVENT, AGGREGATE>>(dc);
        returnValue.add(partition);
      }

      return returnValue;
    }

  }

  /**
   * Kryo has an issue in prior to version 2.23 where it does not honor
   * KryoSerializer implementation.
   *
   * So we provide serializer in a different way. This code will not be
   * needed after 2.23 is released.
   *
   * It seems that a few things are still broken in 2.23.0
   * hint: map vs externalizable interface which needs to be evaluated.
   *
   * @param <T>
   */
  public static class ExternalizableSerializer<T extends Externalizable> extends Serializer<T>
  {
    @Override
    public void write(Kryo kryo, Output output, T object)
    {
      try {
        ObjectOutputStream stream;
        object.writeExternal(stream = new ObjectOutputStream(output));
        stream.flush();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type)
    {
      T object = kryo.newInstance(type);
      kryo.reference(object);
      try {
        object.readExternal(new ObjectInputStream(input));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      return object;
    }

  }

  /**
   * don't use the ExternalizableSerializer any more, use's platform's serializer
   */
  static class AggregatorMap<EVENT, AGGREGATE extends AggregateEvent> extends Object2ObjectOpenCustomHashMap<EVENT, AGGREGATE>
  {
    transient Aggregator<EVENT, AGGREGATE> aggregator;

    @SuppressWarnings("PublicConstructorInNonPublicClass")
    public AggregatorMap()
    {
      /* Needed for Serialization */
      super(null);
      aggregator = null;
    }

    AggregatorMap(Aggregator<EVENT, AGGREGATE> aggregator)
    {
      super(aggregator);
      this.aggregator = aggregator;
    }
    
    AggregatorMap(Aggregator<EVENT, AGGREGATE> aggregator, int initialCapacity)
    {
      super(initialCapacity, aggregator);
      this.aggregator = aggregator;
    }

    public void add(EVENT tuple, int aggregatorIdx)
    {
      AGGREGATE aggregateEvent = get(tuple);
      if (aggregateEvent == null) {
        aggregateEvent = aggregator.getGroup(tuple, aggregatorIdx);
        put(tuple, aggregateEvent);
      }

      aggregator.aggregate(aggregateEvent, tuple);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AggregatorMap)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      AggregatorMap<?, ?> that = (AggregatorMap<?, ?>)o;

      if (aggregator != null ? !aggregator.equals(that.aggregator) : that.aggregator != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = super.hashCode();
      result = 31 * result + (aggregator != null ? aggregator.hashCode() : 0);
      return result;
    }

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(AggregatorMap.class);
    private static final long serialVersionUID = 201311171410L;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DimensionsComputation)) {
      return false;
    }

    DimensionsComputation<?, ?> that = (DimensionsComputation<?, ?>)o;

    return Arrays.equals(incrementalAggregatorMaps, that.incrementalAggregatorMaps);

  }

  @Override
  public int hashCode()
  {
    return incrementalAggregatorMaps != null ? Arrays.hashCode(incrementalAggregatorMaps) : 0;
  }

  private static final Logger logger = LoggerFactory.getLogger(DimensionsComputation.class);

}
