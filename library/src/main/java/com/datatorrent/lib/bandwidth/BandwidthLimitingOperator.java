package com.datatorrent.lib.bandwidth;

import com.datatorrent.api.Operator;

/**
 * Operator which limits bandwidth consumption. It should have instance of BandwidthManager.
 */
public interface BandwidthLimitingOperator extends Operator
{
  public BandwidthManager getBandwidthManager();
}
