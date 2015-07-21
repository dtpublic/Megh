/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.statistics;

import gnu.trove.map.hash.THashMap;

import java.lang.reflect.Array;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Maps;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.statistics.DimensionsComputation.AggregatorMap;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * A {@link Unifier} implementation for {@link DimensionsComputation}.<br/>
 * <p>
 * @displayName Dimension Computation Unifier Implementation
 * @category Statistics
 * @tags event, dimension, aggregation, computation
 * @param <EVENT>
 * @since 0.9.4
 */
public class DimensionsComputationUnifierImpl<EVENT, AGGREGATE extends DimensionsComputation.AggregateEvent> extends BaseOperator implements Operator.Unifier<AGGREGATE>
{
  @NotNull
  private DimensionsComputation.Aggregator<EVENT, AGGREGATE>[] aggregators;
  @NotNull
  private THashMap<AGGREGATE, AGGREGATE> aggregatorMaps[];
  
  /**
   * Output port that emits an aggregate of events.
   */
  public final transient DefaultOutputPort<AGGREGATE> output = new DefaultOutputPort<AGGREGATE>();

  public DimensionsComputationUnifierImpl()
  {
    /** for kryo serialization */
    aggregators = null;
    aggregatorMaps = null;
  }

  /**
   * Sets the aggregators.
   *
   * @param aggregators
   */
  public void setAggregators(@Nonnull DimensionsComputation.Aggregator<EVENT, AGGREGATE>[] aggregators)
  {
    this.aggregators = aggregators;
    
    @SuppressWarnings("unchecked")
    THashMap<AGGREGATE, AGGREGATE>[] newInstance = (THashMap<AGGREGATE, AGGREGATE>[]) Array.newInstance(THashMap.class, aggregators.length);
    aggregatorMaps = newInstance;
    for (int i = aggregators.length; i-- > 0; ) {
      aggregatorMaps[i] = new THashMap<AGGREGATE, AGGREGATE>();
    }
  }

  @Override
  public void process(AGGREGATE tuple)
  {
    int aggregatorIdx = tuple.getAggregatorIndex();
    AGGREGATE destination = aggregatorMaps[aggregatorIdx].get(tuple);
    if (destination == null) {
      aggregatorMaps[aggregatorIdx].put(tuple, tuple);
    }
    else {
      aggregators[aggregatorIdx].aggregate(destination, tuple);
    }
  }

  public void endWindow()
  {
    for (THashMap<AGGREGATE, AGGREGATE> map : aggregatorMaps) {
      for (AGGREGATE value : map.values()) {
        output.emit(value);
      }
      map.clear();
    }
  }
}
