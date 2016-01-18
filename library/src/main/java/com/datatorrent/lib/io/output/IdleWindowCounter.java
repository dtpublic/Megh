/*
 *  Copyright (c) 2016 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.output;

import java.io.IOException;

import com.datatorrent.common.util.BaseOperator;

/**
 * Abstract operator to monitor idle windows and taking custom action when
 * threshold is reached.
 *
 * Derived classes must call markActivity() when it is doing some processing.
 * This is typically called from process() on input ports
 *
 */
public abstract class IdleWindowCounter extends BaseOperator
{
  protected int timeoutWindowCount;
  private int idleCount;
  private boolean noActivity;

  public IdleWindowCounter()
  {
    timeoutWindowCount = getIdleWindowThresholdDefault();
  }

  abstract protected int getIdleWindowThresholdDefault();

  protected void markActivity()
  {
    noActivity = false;
  }

  @Override
  public void beginWindow(long windowId)
  {
    noActivity = true;
  }

  /**
   * Indicator for operation or data in progress
   * 
   * @return
   */
  protected abstract boolean hasMoreWork();

  /**
   * Custom action to be taken when idle window threshold is reached.
   * 
   * @throws IOException
   */
  protected abstract void idleWindowThresholdReached();

  @Override
  public void endWindow()
  {
    if (noActivity && !hasMoreWork()) {
      idleCount++;
    } else {
      idleCount = 0;
    }
    if (idleCount > timeoutWindowCount) {
      idleWindowThresholdReached();
      idleCount = 0;
    }
  }

  /**
   * @return the timeoutWindowCount
   */
  public int getTimeoutWindowCount()
  {
    return timeoutWindowCount;
  }

  /**
   * @param timeoutWindowCount
   *          the timeoutWindowCount to set
   */
  public void setTimeoutWindowCount(int timeoutWindowCount)
  {
    this.timeoutWindowCount = timeoutWindowCount;
  }

}
