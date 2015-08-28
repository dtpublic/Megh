package com.datatorrent.alerts;

public interface ActionHandler<T extends ActionTuple> {
  public void handle( T tuple );
}
