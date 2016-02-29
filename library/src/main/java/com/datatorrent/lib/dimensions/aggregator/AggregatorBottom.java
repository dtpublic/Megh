package com.datatorrent.lib.dimensions.aggregator;

public class AggregatorBottom extends AbstractTopBottomAggregator
{
  @Override
  protected boolean shouldReplaceResultElement(int resultCompareToInput)
  {
    return resultCompareToInput < 0;
  }
}
