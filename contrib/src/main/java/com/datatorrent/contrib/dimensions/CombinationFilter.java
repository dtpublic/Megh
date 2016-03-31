/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.Map;
import java.util.Set;

/**
 * This interface provides method to filter the input keyToValues
 * 
 * @since 3.3.0
 */
public interface CombinationFilter
{
  /**
   * filter the input keyToValues
   * @param keyToValues the source keyToValues to be filtered.
   * @return the filtered keyToValues map
   */
  public Map<String, Set<Object>> filter(Map<String, Set<Object>> keyToValues);
}
