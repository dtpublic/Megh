/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.ingestion.io;

import com.datatorrent.api.Operator;
import com.datatorrent.apps.ingestion.lib.BandwidthManager;

/**
 * Operator which limits bandwidth consumption. It should have instance of BandwidthManager.
 */
public interface BandwidthLimitingOperator extends Operator
{
  public BandwidthManager getBandwidthManager();
}
