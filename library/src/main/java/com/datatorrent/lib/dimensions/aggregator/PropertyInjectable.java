/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

public interface PropertyInjectable
{
  public void injectProperty(String name, Object value);
}
