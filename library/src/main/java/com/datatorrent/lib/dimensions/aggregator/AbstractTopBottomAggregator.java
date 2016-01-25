/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

public abstract class AbstractTopBottomAggregator<T> extends SimpleCompositeAggregator<T> implements PropertyInjectable
{
  public static final String PROP_COUNT = "count";
  protected int count;

  public AbstractTopBottomAggregator<T> withCount(int count)
  {
    this.setCount(count);
    return this;
  }

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }

  @Override
  public void injectProperty(String name, Object value)
  {
    if (!PROP_COUNT.equals(name))
      throw new IllegalArgumentException("Invalid property name " + name + ". Only support " + PROP_COUNT);
    if (value == null)
      return;
    int countValue = 0;
    if (value instanceof String) {
      countValue = Integer.parseInt((String)value);
    } else if (value instanceof Integer)
      countValue = (Integer)value;
    else
      throw new IllegalArgumentException(
          "Invalid property value type " + value.getClass() + ". Only support Integer and String.");
    this.setCount(countValue);
  }

  @Override
  public int hashCode()
  {
    return embededAggregator.hashCode()*31 + count;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    
    AbstractTopBottomAggregator other = (AbstractTopBottomAggregator)obj;
    if (embededAggregator != other.embededAggregator
        && (embededAggregator == null || embededAggregator.equals(other.embededAggregator)))
      return false;
    if (count != other.count)
      return false;
    return true;
  }

}
