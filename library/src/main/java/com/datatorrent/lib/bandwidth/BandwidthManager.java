package com.datatorrent.lib.bandwidth;
/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */

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
  private long bandwidthLimit = Long.MAX_VALUE;
  private transient long currentBandwidthConsumption;
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
    scheduler.scheduleAtFixedRate(new BandwidthAccumulator(), 1, 1, TimeUnit.SECONDS);
  }

  public boolean canConsumeBandwidth()
  {
    if (!isBandwidthRestricted()) {
      return true;
    }
    synchronized (lock) {
      if (currentBandwidthConsumption >= 0) {
        return true;
      }
    }
    return false;
  }

  public void consumeBandwidth(long sentTupleSize)
  {
    if (isBandwidthRestricted()) {
      synchronized (lock) {
        currentBandwidthConsumption -= sentTupleSize;
      }
    }
  }

  public boolean isBandwidthRestricted()
  {
    if (bandwidthLimit == Long.MAX_VALUE) {
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
          if (currentBandwidthConsumption < 0) {
            currentBandwidthConsumption += bandwidthLimit;
          }
          LOG.debug("Available bandwidth: " + currentBandwidthConsumption);
        }
      }
    }
  }
}

