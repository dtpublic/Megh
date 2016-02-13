/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;

/**
 * this factory is implemented for support TOPN and BOTTOMN right now.
 * we are not clear what other composite aggregator could be, provide interface here.
 * assume Composite only embed one aggregate and with some properties
 */
public interface CompositeAggregatorFactory
{
  public String getCompositeAggregatorName(String aggregatorType, String embededAggregatorName, Map<String, Object> properties);
  public <T> SimpleCompositeAggregator<T> createCompositeAggregator(String aggregatorType, T embededAggregator, Map<String, Object> properties);
 
}
