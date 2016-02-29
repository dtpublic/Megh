/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

public class AggregatorTop extends AbstractTopBottomAggregator
{
  @Override
  protected boolean shouldReplaceResultElement(int resultCompareToInput)
  {
    return resultCompareToInput > 0;
  }
}
