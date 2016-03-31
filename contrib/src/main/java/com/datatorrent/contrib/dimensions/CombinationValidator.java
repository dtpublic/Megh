/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface provides methods to valify key and value 
 * 
 * @since 3.3.0
 *
 * @param <K> type of Key
 * @param <V> type of Value
 */
public interface CombinationValidator<K, V>
{
  /**
   * order input keys
   * @param keys The keys need to be ordered
   * @return The ordered keys
   */
  public List<K> orderKeys(List<K> keys);
  
  /**
   * check if the key and value is valid entry
   * 
   * @param combinedKeyValues
   * @param key
   * @param value
   * @return true if key value are valid
   */
  public boolean isValid(Map<K, Set<V>> combinedKeyValues, K key, V value );
}
