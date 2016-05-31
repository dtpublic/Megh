/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.bucket.BucketStore.ExpirableBucketStore;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerPOJOImpl;
import com.datatorrent.lib.io.ConsoleOutputOperator;

public class DeduperPOJOTestStreamCodec
{
  public static final int NUM_DEDUP_PARTITIONS = 5;
  private static boolean testFailed = false;

  public static class TestDedupApp implements StreamingApplication
  {

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      TestGenerator gen = dag.addOperator("Generator", new TestGenerator());
      TestDeduper dedup = dag.addOperator("Deduper", new TestDeduper());
      TimeBasedBucketManagerPOJOImpl bucketManager = new TimeBasedBucketManagerPOJOImpl();
      ExpirableBucketStore<Object> store = new ExpirableHdfsBucketStore<>();
      bucketManager.setBucketStore(store);
      bucketManager.setKeyExpression("$.id");
      bucketManager.setTimeExpression("$.eventTime");
      dedup.setBucketManager(bucketManager);
      ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
      dag.addStream("Generator to Dedup", gen.output, dedup.input);
      dag.addStream("Dedup to Console", dedup.output, console.input);
      dag.setInputPortAttribute(dedup.input, Context.PortContext.TUPLE_CLASS, TestEvent.class);
      dag.setOutputPortAttribute(dedup.output, Context.PortContext.TUPLE_CLASS, TestEvent.class);
      dag.setAttribute(dedup, Context.OperatorContext.PARTITIONER, 
          new StatelessPartitioner<DeduperPOJOImpl>(NUM_DEDUP_PARTITIONS));
    }
  }

  public static class TestDeduper extends DeduperPOJOImpl
  {
    int operatorId;
    boolean started = false;
    HashMap<Integer, Integer> partitionMap = Maps.newHashMap();

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      operatorId = context.getId();
    }

    @Override
    protected void processTuple(Object tuple)
    {
      TestEvent event = (TestEvent)tuple;
      if (partitionMap.containsKey(event.id)) {
        if (partitionMap.get(event.id) != operatorId) {
          testFailed = true;
          throw new RuntimeException("Wrong tuple assignment");
        }
      } else {
        partitionMap.put(event.id, operatorId);
      }
    }
  }

  public static class TestGenerator extends BaseOperator implements InputOperator
  {

    public final transient DefaultOutputPort<TestEvent> output = new DefaultOutputPort<>();
    private Random r = new Random();

    @Override
    public void emitTuples()
    {
      TestEvent event = new TestEvent();
      event.id = r.nextInt(100);
      output.emit(event);
    }
  }

  public static class TestEvent
  {
    int id;
    Date eventTime;

    public TestEvent()
    {
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public Date getEventTime()
    {
      return eventTime;
    }

    public void setEventTime(Date eventTime)
    {
      this.eventTime = eventTime;
    }
  }

  @Test
  public void testDeduperStreamCodec()
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new TestDedupApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10 * 1000); // runs for 10 seconds and quits
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("constraint violations: " + e);
    }
    Assert.assertFalse(testFailed);
  }
}
