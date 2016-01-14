/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

/**
 * this factory is implemented for support TOPN and BOTTOMN right now.
 *
 */
public class CompositeAggregatorFactory
{
  public static final String TOPN_BOTTOMN_NAME_TEMPLATE = "%s-%s-%d";
  public static String getCompositeAggregatorName(String aggregatorType, String embededAggregatorName, int count)
  {
    if(AggregatorCompositeType.valueOf(aggregatorType) == null)
      throw new IllegalArgumentException("Invalid composite aggregator name: " + aggregatorType);
    return String.format(TOPN_BOTTOMN_NAME_TEMPLATE, aggregatorType, embededAggregatorName, count);
  }
  
  public static <T> AbstractTopBottomAggregator<T> createTopBottomAggregator(String aggregatorType, T embededAggregator, int count)
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
}
