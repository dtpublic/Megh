/**
 * Copyright (c) 2016 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.dimensions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.stream.DevNull;

@ApplicationAnnotation(name = "FaultTolerantTestApp")
public class FaultTolerantTestApp implements StreamingApplication
{
  protected static final int tupleSize = 2000;
  protected static final int tuplePerWindow = 500;
  protected static final int expectedWindows = tupleSize / tuplePerWindow + tupleSize % tuplePerWindow;

  protected int applicationWindowCount = 4;
  protected int checkpointWindowCount = 2;
  protected static int deployCount = 0;
  protected static int tupleCount = 0;
  
  public FaultTolerantTestApp(int applicationWindowCount, int checkpointWindowCount)
  {
    this.applicationWindowCount = applicationWindowCount;
    this.checkpointWindowCount = checkpointWindowCount;
  }

  /**
   * The tuple class must public
   *
   */
  public static class TestPojo
  {
    public TestPojo()
    {
      name = "name";
      count = 1;
    }
    
    public String name;
    public long count;
    
    public long getTime()
    {
      return System.currentTimeMillis();
    }
  }
  
  protected static class TestPojoGenerator implements InputOperator, Operator.CheckpointNotificationListener
  {
    private static final transient Logger logger = LoggerFactory.getLogger(TestPojoGenerator.class);
    
    public final transient DefaultOutputPort<TestPojo> outputPort = new DefaultOutputPort<>();

    
    protected transient TestPojo testPojo = new TestPojo();

    protected transient boolean checkPointed = false;
    
    protected int emittedTuplesInWindow = 0;
    @Override
    public void emitTuples()
    {
      if (emittedTuplesInWindow < tuplePerWindow) {
        outputPort.emit(testPojo);
        ++tupleCount;
        ++emittedTuplesInWindow;
      }
      
      try {
        Thread.sleep(1);
      } catch (Exception e) {
        //ignore
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      emittedTuplesInWindow = 0;
    }

    //protected transient int endWindowCount = 0;
    @Override
    public void endWindow()
    {
      if (tupleCount == tupleSize) {
        throw new ShutdownException();
      }
      
      if (deployCount == 1 && checkPointed) {
        throw new RuntimeException();
      }
    }

    @Override
    public void setup(OperatorContext context)
    {
      ++deployCount;
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void checkpointed(long windowId)
    {
      checkPointed = true;
    }

    @Override
    public void committed(long windowId)
    {
    }

    @Override
    public void beforeCheckpoint(long windowId)
    {
    }
  }
  
  
  protected static int setupTimes = 0;
  public static class TestDimensionsPOJO extends DimensionsComputationFlexibleSingleSchemaPOJO
  {
    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      ++setupTimes;
    }
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    deployCount = 0;
    setupTimes = 0;
    
    String eventSchema = SchemaUtils.jarResourceFileToString("FaultTolerantTestApp.json");

    TestPojoGenerator generator = new TestPojoGenerator();

    dag.addOperator("generator", generator);

    TestDimensionsPOJO dimensions = new TestDimensionsPOJO();
    dag.addOperator("dimensions", dimensions);
    dimensions.setConfigurationSchemaJSON(eventSchema);

    dag.setAttribute(dimensions, Context.OperatorContext.APPLICATION_WINDOW_COUNT, applicationWindowCount);
    dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, checkpointWindowCount);

    DevNull<Object> outputOperator = new DevNull<>();
    dag.addOperator("output", outputOperator);

    dag.addStream("dimensionsStream", generator.outputPort, dimensions.input);
    dag.addStream("outputStream", dimensions.output, outputOperator.data);
  }
}
