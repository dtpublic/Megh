package com.datatorrent.alerts;

public interface ActionHandler {
  public void handle( ActionTuple tuple );
}
