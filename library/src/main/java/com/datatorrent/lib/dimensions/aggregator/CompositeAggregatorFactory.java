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
  /**
   * check if aggregatorName is a valid composite aggregator name or not.
   * @param aggregatorName
   * @return
   */
  //public boolean isValidCompositeAggregatorName(String aggregatorName);
  
  /**
   * get composite aggregator name based on composite aggregator information
   * @param aggregatorType
   * @param embedAggregatorName
   * @param properties
   * @return
   */
  public String getCompositeAggregatorName(String aggregatorType, String embedAggregatorName, Map<String, Object> properties);
  
  /**
   * create composite aggregator name based on composite aggregator information
   * @param aggregatorType
   * @param embedAggregatorName
   * @param properties
   * @return
   */
  public <T> CompositeAggregator createCompositeAggregator(String aggregatorType, String embedAggregatorName, Map<String, Object> properties);

}
