/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.ingestion.lib;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

/**
 * BandwidthManager keeps track of bandwidth consumption and provides limit on maximum bandwidth that can be consumed at
 * any moment. This accumulates bandwidth upto certain limits so that accumulated bandwidth can be used over a period of
 * time.
 */
public class BandwidthManager implements Component<Context.OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(BandwidthManager.class);
  /**
   * Maximum bandwidth that can be consumed in bytes/sec
   */
  private long bandwidthLimit;
  private transient long accumuldatedBandwidth;
  private long bandwidthAccumulationLimit;
  private long curretTupleSize;
  private final transient ScheduledExecutorService scheduler;
  private final transient Object lock = new Object();

  public BandwidthManager()
  {
    scheduler = Executors.newScheduledThreadPool(1);
  }

  BandwidthManager(ScheduledExecutorService scheduler)
  {
    this.scheduler = scheduler;
  }

  @Override
  public void setup(OperatorContext context)
  {
    bandwidthAccumulationLimit = bandwidthLimit * 100;
    scheduler.scheduleAtFixedRate(new BandwidthAccumulator(), 1, 1, TimeUnit.SECONDS);
  }

  public boolean canConsumeBandwidth(long tupleSize)
  {
    curretTupleSize = tupleSize;
    if (!isBandwidthRestricted()) {
      return true;
    }
    synchronized (lock) {
      if (tupleSize <= accumuldatedBandwidth) {
        return true;
      }
    }
    return false;
  }

  public void consumeBandwidth(long sentTupleSize)
  {
    if (isBandwidthRestricted()) {
      synchronized (lock) {
        accumuldatedBandwidth -= sentTupleSize;
      }
    }
  }

  private boolean isBandwidthRestricted()
  {
    if (bandwidthLimit == 0) {
      return false;
    }
    return true;
  }

  /**
   * get maximum bandwidth that can be consumed in bytes/sec
   *
   * @return
   */
  public long getBandwidth()
  {
    return bandwidthLimit;
  }

  /**
   * Set maximum bandwidth that can be consumed in bytes/sec
   *
   * @param bandwidth
   */
  public void setBandwidth(long bandwidth)
  {
    this.bandwidthLimit = bandwidth;
    LOG.info("Bandwidth limit is set to: " + bandwidth + " bytes/sec");
  }

  @Override
  public void teardown()
  {
    scheduler.shutdownNow();
  }

  class BandwidthAccumulator implements Runnable
  {
    @Override
    public void run()
    {
      if (isBandwidthRestricted()) {
        synchronized (lock) {
          if (accumuldatedBandwidth < (curretTupleSize > bandwidthAccumulationLimit ? curretTupleSize : bandwidthAccumulationLimit)) {
            accumuldatedBandwidth += bandwidthLimit;
          }
        }
        LOG.debug("Available bandwidth: " + accumuldatedBandwidth);
      }
    }
  }

}
