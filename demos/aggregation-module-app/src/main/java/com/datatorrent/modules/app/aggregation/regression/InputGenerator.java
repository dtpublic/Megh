/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app.aggregation.regression;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.schemas.TimeBucket;

import net.sf.jagg.AggregateFunction;
import net.sf.jagg.Aggregations;
import net.sf.jagg.Aggregator;
import net.sf.jagg.model.AggregateValue;

public class InputGenerator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(InputGenerator.class);

  public final transient DefaultOutputPort<POJO> out = new DefaultOutputPort<>();

  private Random rand = new Random();

  private int maxPerWindow = 1000;
  private int currentWindowEmitCount = 0;
  private long windowId = 0;

  @Override
  public void emitTuples()
  {
    if (currentWindowEmitCount < maxPerWindow) {
      POJO tuple = generatePOJO();
      out.emit(tuple);
      currentWindowEmitCount++;
      logger.info("InputTuple: " + this.windowId + "-" + TimeBucket.MINUTE.roundDown(tuple.getTime()) + "-" + tuple.getTime() + "," + tuple.getAdvertiserId() + "," + tuple.getPublisherId() + "," +
        tuple.getLocationId() + "," + tuple.getClicks() + "," + tuple.getCost());
    }
    else {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        logger.warn("Failed to sleep. Continuing..", e);
      }
    }
  }

  private POJO generatePOJO()
  {
    POJO p = new POJO();
    p.setAdvertiserId(rand.nextInt(5));
    p.setLocationId(rand.nextInt(5));
    p.setPublisherId(rand.nextInt(5));
    p.setClicks(1);
    p.setCost(100.0d);
    p.setTime(System.currentTimeMillis());
    return p;
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowEmitCount = 0;
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public int getMaxPerWindow()
  {
    return maxPerWindow;
  }

  public void setMaxPerWindow(int maxPerWindow)
  {
    this.maxPerWindow = maxPerWindow;
  }
}
