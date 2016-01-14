package com.datatorrent.lib.dimensions.aggregator;

public interface PropertyInjectable
{
  public void injectProperty(String name, Object value);
}
