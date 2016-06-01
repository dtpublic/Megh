/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.lib.laggards;

import java.util.Date;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.stram.engine.PortContext;

/**
 * Tests for Laggards
 */

public class LaggardsTest
{
  private static class DummyLaggards extends LaggardsOperator
  {
    private long currentTime;

    @Override
    public long fetchCurrentTime()
    {
      return currentTime;
    }

    public void setCurrentTime(long currentTime)
    {
      this.currentTime = currentTime;
    }
  }

  public static class DummyPOJO
  {
    private Date time = new Date();

    public Date getTime()
    {
      return time;
    }

    public void setTime(Date time)
    {
      this.time = time;
    }

    public void setUnixTime(long _time)
    {
      time.setTime(_time);
    }
  }

  private static DummyLaggards laggards;
  private static DummyPOJO data;

  private void normalTuplesTest()
  {
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time * 1000);
    data.setUnixTime(curr_time * 1000);
    laggards.in.process(data);
    Assert.assertEquals("normal tuples", 1, laggards.normalTuples);
  }

  private void laggardsTuplesTest()
  {
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time * 1000);
    data.setUnixTime(1231199 * 1000);
    laggards.in.process(data);
    Assert.assertEquals("laggards tuples", 1, laggards.laggardsTuples);
  }

  private void lateLaggardsTuplesTest()
  {
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time * 1000);
    data.setUnixTime(1227599 * 1000);
    laggards.in.process(data);
    Assert.assertEquals("late laggards tuples", 1, laggards.lateLaggardsTuples);
  }

  private void bufferTimeTuplesTest()
  {
    Random randomGenerator = new Random();
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time * 1000);

    data.setUnixTime((curr_time + randomGenerator.nextInt(60)) * 1000);
    laggards.in.process(data);
    Assert.assertEquals("buffer time tuples", 1, laggards.normalTuples);
  }

  private void errorTuplesTest()
  {
    Random randomGenerator = new Random();
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time * 1000);

    /* Ahead in time */
    data.setUnixTime((curr_time + 60 + 1 + randomGenerator.nextInt(60)) * 1000);
    laggards.in.process(data);

    /* late laggards */
    data.setUnixTime(1227599 * 1000);
    laggards.in.process(data);
    Assert.assertEquals("error tuples", 2, laggards.errorTuples);
  }

  @Test
  public void testLaggards()
  {
    LOG.debug("start round 0");
    laggards.beginWindow(0);

    normalTuplesTest();

    laggards.endWindow();
    LOG.debug("end round 0");

    LOG.debug("start round 1");
    laggards.beginWindow(1);

    laggardsTuplesTest();

    laggards.endWindow();
    LOG.debug("end round 1");

    LOG.debug("start round 2");
    laggards.beginWindow(2);

    lateLaggardsTuplesTest();

    laggards.endWindow();
    LOG.debug("end round 2");

    LOG.debug("start round 3");
    laggards.beginWindow(3);

    bufferTimeTuplesTest();

    laggards.endWindow();
    LOG.debug("end round 3");

    LOG.debug("start round 4");
    laggards.beginWindow(3);

    errorTuplesTest();

    laggards.endWindow();
    LOG.debug("end round 4");

  }

  @BeforeClass
  public static void setup()
  {
    data = new DummyPOJO();
    laggards = new DummyLaggards();
    laggards.setup(null);

    Attribute.AttributeMap in = new Attribute.AttributeMap.DefaultAttributeMap();
    in.put(Context.PortContext.TUPLE_CLASS, DummyPOJO.class);
    laggards.in.setup(new PortContext(in, null));
  }

  @AfterClass
  public static void teardown()
  {
    laggards.teardown();
  }

  private static final Logger LOG = LoggerFactory.getLogger(LaggardsTest.class);
}
