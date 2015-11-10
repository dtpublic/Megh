/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedToPojo;

import javax.validation.constraints.Min;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class InputDataGeneratorOperator extends BaseOperator implements InputOperator
{

  private String[] data = {
      "1234,98233,adxyz,0.2,2015-03-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,yes",
      "1234,98233,adxyz,0.2,2015-30-08 02:37:12,11/12/2012,12,y,,CAMP_AD,Y,no",
      "adId,campaignId,adName,bidPrice,startDate,endDate,securityCode,active,optimized,"
          + "parentCampaign,weatherTargeted,no" };

  private int maxCountOfWindows = Integer.MAX_VALUE;
  @Min(1)
  private int tuplesBlast = 500;

  public InputDataGeneratorOperator()
  {
  }

  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  public int getMaxCountOfWindows()
  {
    return maxCountOfWindows;
  }

  public void setMaxCountOfWindows(int maxCountOfWindows)
  {
    this.maxCountOfWindows = maxCountOfWindows;
  }

  public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  public void setTuplesBlast(int tuplesBlast)
  {
    this.tuplesBlast = tuplesBlast;
  }

  @Override
  public void endWindow()
  {
    if (--maxCountOfWindows == 0) {
      throw new RuntimeException(new InterruptedException("Finished generating data."));
    }
  }

  @Override
  public void emitTuples()
  {
    int i = 0;
    while (i < tuplesBlast) {
      if (output.isConnected()) {
        for(String d : data){
          output.emit(d.getBytes());
        }
      }
      i++;
    }
  }
}
