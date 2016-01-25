/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Map;

public abstract class AbstractCompositeAggregatorFactory implements CompositeAggregatorFactory
{
  public static final String NAME_TEMPLATE = "%s-%s-%s";
  public static final String PROPERTY_SEPERATOR = "_";
  public static final String PROPERTY_VALUE_SEPERATOR = "|";
  
  @Override
  public String getCompositeAggregatorName(String aggregatorType, String embededAggregatorName,
      Map<String, Object> properties)
  {
    return String.format(NAME_TEMPLATE, aggregatorType, embededAggregatorName, getNamePartialForProperties(properties));
  }

  protected String getNamePartialForProperties(Map<String, Object> properties)
  {
    if(properties.size() == 1)
      return properties.values().iterator().next().toString();
    StringBuilder sb = new StringBuilder();
    for(Map.Entry<String, Object> entry : properties.entrySet())
    {
      sb.append(entry.getKey()).append(PROPERTY_VALUE_SEPERATOR).append(entry.getValue()).append(PROPERTY_SEPERATOR);
    }
    //delete the last one (PROPERTY_SEPERATOR)
    return sb.deleteCharAt(sb.length()-1).toString();
  }
}

