/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.modules.app;

import java.util.Random;

import com.sun.org.apache.bcel.internal.util.Objects;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

class RandomGenerator extends RandomEventGenerator
{
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
  private final Random random = new Random();
  private long noOfWindows = 0;
  private long noOfWindowsToEscape = 0;
  private int count = 0;
  private String initialOffset;
  private boolean disableGenerate = true;

  @Override
  public void setup(Context.OperatorContext context)
  {
    noOfWindowsToEscape = 120;
    super.setup(context);
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    super.beginWindow(windowId);
    noOfWindows++;
  }

  @Override
  public void emitTuples()
  {
    if(disableGenerate) {
      return;
    }
    if(initialOffset.equals(KafkaValidationModule.DEFAULTOFFSET)) {
      if(noOfWindows < noOfWindowsToEscape) {
        return;
      }
    }

    int range = getMaxvalue() - getMinvalue() + 1;
    while (count < getTuplesBlast()) {
      int rval = getMinvalue() + random.nextInt(range);
      if (integer_data.isConnected()) {
        integer_data.emit(rval);
      }
      if (string_data.isConnected()) {
        string_data.emit(Integer.toString(rval));
      }
      if (output.isConnected()) {
        output.emit(rval);
      }
      count++;
    }
  }

  public String getInitialOffset()
  {
    return initialOffset;
  }

  public void setInitialOffset(String initialOffset)
  {
    this.initialOffset = initialOffset;
  }

  public boolean isDisableGenerate()
  {
    return disableGenerate;
  }

  public void setDisableGenerate(boolean disableGenerate)
  {
    this.disableGenerate = disableGenerate;
  }
}
