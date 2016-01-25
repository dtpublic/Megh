/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * The DefaultCompositeAggregatorFactory find the specific factory according to the aggregator type
 * and delegate to the specific factory.
 * 
 */
public class DefaultCompositeAggregatorFactory implements CompositeAggregatorFactory
{
  public static final DefaultCompositeAggregatorFactory defaultInst = new DefaultCompositeAggregatorFactory()
      .addFactory(AggregatorCompositeType.TOPN.name(), TopBottomAggregatorFactory.defaultInstance)
      .addFactory(AggregatorCompositeType.BOTTOMN.name(), TopBottomAggregatorFactory.defaultInstance);
  
  protected Map<String, CompositeAggregatorFactory> factoryRepository = Maps.newHashMap();
  
  //add this factory to create aggregator in a generic way by using reflection 
  protected CompositeAggregatorFactory genericCompositeAggregatorFactory;
  
  @Override
  public String getCompositeAggregatorName(String aggregatorType, String embededAggregatorName,
      Map<String, Object> properties)
  {
    return findSpecificFactory(aggregatorType).getCompositeAggregatorName(aggregatorType, embededAggregatorName, properties);
  }

  @Override
  public <T> SimpleCompositeAggregator<T> createCompositeAggregator(String aggregatorType, T embededAggregator,
      Map<String, Object> properties)
  {
    return findSpecificFactory(aggregatorType).createCompositeAggregator(aggregatorType, embededAggregator, properties);
  }

  protected CompositeAggregatorFactory findSpecificFactory(String aggregatorType)
  {
    CompositeAggregatorFactory specificFactory = factoryRepository.get(aggregatorType);
    return specificFactory != null ? specificFactory : genericCompositeAggregatorFactory;
  }
  
  public DefaultCompositeAggregatorFactory addFactory(String aggregatorType, CompositeAggregatorFactory factory)
  {
    factoryRepository.put(aggregatorType, factory);
    return this;
  }
}
