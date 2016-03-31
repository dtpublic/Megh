/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.util.Map;

import com.datatorrent.lib.appdata.schemas.ResultFormatter;

/**
 * This class should move to Malhar in the future
 *
 */
public class MapResultFormatter extends ResultFormatter
{
  private static final long serialVersionUID = -5870574823013121380L;
  protected ResultFormatter primitiveTypeFormatter;
  
  public MapResultFormatter(){}
  
  public MapResultFormatter(ResultFormatter primitiveTypeFormatter)
  {
    setPrimitiveTypeFormatter(primitiveTypeFormatter);
  }
  
  
  @Override
  public String format(Object object)
  {
    if(object instanceof Map)
      return format((Map)object);
    
    return primitiveTypeFormatter.format(object);
  }

  /**
   * support both key and value are primitive
   * @param map
   * @return
   */
  protected String format(Map<Object, Object> map)
  {
    StringBuilder formatted = new StringBuilder();
    formatted.append("[");
    for(Map.Entry<Object, Object> entry : map.entrySet())
    {
      formatted.append("{");
      formatted.append(primitiveTypeFormatter.format(entry.getKey())).append(":").append(primitiveTypeFormatter.format(entry.getValue()));
      formatted.append("},");
    }
    if(map.size() > 0)
      formatted.deleteCharAt(formatted.length()-1);
    formatted.append("]");
    return formatted.toString();
  }

  public ResultFormatter getPrimitiveTypeFormatter()
  {
    return primitiveTypeFormatter;
  }

  public void setPrimitiveTypeFormatter(ResultFormatter primitiveTypeFormatter)
  {
    this.primitiveTypeFormatter = primitiveTypeFormatter;
  }
}
