/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.lib.laggards;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for Laggards
 */

public class LaggardsTest
{
  private static final Logger logger = LoggerFactory.getLogger(LaggardsTest.class);

  private static class DummyLaggards extends LaggardsOperator
  {
    private long currentTime;

    @Override
    protected void updateTime()
    {
      long tumblingWindowTime = getTumblingWindowTime();
      long laggardsWindowTime = getLaggardsWindowTime();

      referenceTime = currentTime;
      currentWindowStartTime = referenceTime - (referenceTime % tumblingWindowTime);
      laggardsStartTime = currentWindowStartTime - (tumblingWindowTime > laggardsWindowTime ? tumblingWindowTime : laggardsWindowTime);
    }

    public long getCurrentTime()
    {
      return currentTime;
    }

    public void setCurrentTime(long _currentTime)
    {
      currentTime = _currentTime;
    }
  }

  public static class DummyPOJO
  {
    private static final Logger logger = LoggerFactory.getLogger(DummyPOJO.class);

    private long time;

    public long getTime()
    {
      return time;
    }

    public void setTime(long _time)
    {
      time = _time;
    }
  }

  private static DummyLaggards laggards;
  private static DummyPOJO data;

  private void normalTuplesTest()
  {
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time);
    data.setTime(curr_time);
    laggards.in.process(data);
    Assert.assertEquals("normal tuples", 1, laggards.normalTuples);
  }

  private void laggardsTuplesTest()
  {
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time);
    data.setTime(1231199);
    laggards.in.process(data);
    Assert.assertEquals("laggards tuples", 1, laggards.laggardsTuples);
  }

  private void lateLaggardsTuplesTest()
  {
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time);
    data.setTime(1227599);
    laggards.in.process(data);
    Assert.assertEquals("late laggards tuples", 1, laggards.lateLaggardsTuples);
  }

  private void bufferTimeTuplesTest()
  {
    Random randomGenerator = new Random();
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time);

    data.setTime(curr_time + randomGenerator.nextInt(60));
    laggards.in.process(data);
    Assert.assertEquals("buffer time tuples", 1, laggards.normalTuples);
  }

  private void errorTuplesTest()
  {
    Random randomGenerator = new Random();
    long curr_time = 1234567;
    laggards.setCurrentTime(curr_time);

    /* Ahead in time */
    data.setTime(curr_time + 60 + 1 + randomGenerator.nextInt(60));
    laggards.in.process(data);

    /* late laggards */
    data.setTime(1227599);
    laggards.in.process(data);
    Assert.assertEquals("error tuples", 2, laggards.errorTuples);
  }

  @Test
  public void testLaggards()
  {
    logger.debug("start round 0");
    laggards.beginWindow(0);

    normalTuplesTest();

    laggards.endWindow();
    logger.debug("end round 0");

    logger.debug("start round 1");
    laggards.beginWindow(1);

    laggardsTuplesTest();

    laggards.endWindow();
    logger.debug("end round 1");

    logger.debug("start round 2");
    laggards.beginWindow(2);

    lateLaggardsTuplesTest();

    laggards.endWindow();
    logger.debug("end round 2");

    logger.debug("start round 3");
    laggards.beginWindow(3);

    bufferTimeTuplesTest();

    laggards.endWindow();
    logger.debug("end round 3");

    logger.debug("start round 4");
    laggards.beginWindow(3);

    errorTuplesTest();

    laggards.endWindow();
    logger.debug("end round 4");

  }

  @BeforeClass
  public static void setup()
  {
    data = new DummyPOJO();
    laggards = new DummyLaggards();
    laggards.clazz = DummyPOJO.class;
  }

  @AfterClass
  public static void teardown()
  {
    laggards.teardown();
  }
}
