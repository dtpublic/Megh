package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CombinationValidator<K, V>
{
  public List<K> orderKeys(List<K> keys);
  public boolean isValid(Map<K, Set<V>> combinedKeyValues, K key, V value );
}
