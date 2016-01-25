/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;

public class TopBottomAggregatorFactory extends AbstractCompositeAggregatorFactory
{
  public static final String PROP_COUNT = "count";

  public static final TopBottomAggregatorFactory defaultInstance = new TopBottomAggregatorFactory();
  
  @Override
  public <T> AbstractTopBottomAggregator<T> createCompositeAggregator(String aggregatorType, T embededAggregator,
      Map<String, Object> properties)
  {
    return createTopBottomAggregator(aggregatorType, embededAggregator, getCount(properties));
  }
  
  public <T> AbstractTopBottomAggregator<T> createTopBottomAggregator(String aggregatorType, T embededAggregator, int count)
  {
    AbstractTopBottomAggregator<T> aggregator = null;
    if(AggregatorCompositeType.TOPN == AggregatorCompositeType.valueOf(aggregatorType))
    {
      aggregator = new AggregatorTop<T>();
    }
    if(AggregatorCompositeType.BOTTOMN == AggregatorCompositeType.valueOf(aggregatorType))
    {
      aggregator = new AggregatorBottom<T>();
    }
    if(aggregator == null)
    {
      throw new IllegalArgumentException("Invalid composite type: " + aggregatorType);
    }
    aggregator.setCount(count);
    aggregator.setEmbededAggregator(embededAggregator);
    
    return aggregator;
  }

  protected int getCount(Map<String, Object> properties)
  {
    return Integer.valueOf((String)properties.get(PROP_COUNT));
  }
}
