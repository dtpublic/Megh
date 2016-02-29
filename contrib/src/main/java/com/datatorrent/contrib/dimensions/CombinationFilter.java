package com.datatorrent.contrib.dimensions;

import java.util.Map;
import java.util.Set;

public interface CombinationFilter
{

  public Map<String, Set<Object>> filter(Map<String, Set<Object>> keyToValues);
}
