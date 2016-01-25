/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

/**
 * SimpleCompositAggregator is the aggregator which embed other aggregator
 *
 *
 * @param <T> the type of aggregator, could be OTFAggregator or IncrementalAggregator
 */
public class SimpleCompositeAggregator<T>
{
  protected T embededAggregator;

  public SimpleCompositeAggregator<T> withEmbededAggregator(T embededAggregator)
  {
    this.setEmbededAggregator(embededAggregator);
    return this;
  }
  
  public T getEmbededAggregator()
  {
    return embededAggregator;
  }

  public void setEmbededAggregator(T embededAggregator)
  {
    this.embededAggregator = embededAggregator;
  }
}
