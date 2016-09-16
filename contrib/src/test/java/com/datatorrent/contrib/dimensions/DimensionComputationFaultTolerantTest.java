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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

@RunWith(value = Parameterized.class)
public class DimensionComputationFaultTolerantTest extends FaultTolerantTestApp
{
  public static final transient Logger logger = LoggerFactory.getLogger(DimensionComputationFaultTolerantTest.class);
  protected static final int atMostRunTime = 3 * 60 * 1000;    // 3 minutes
  
  // name attribute is optional, provide an unique name for test
  // multiple parameters, uses Collection<Object[]>
  @Parameters(name = "{index}: applicationWindowCount: {0}; checkpointWindowCount: {1}")
  public static Collection<Integer[]> data()
  {
    return Arrays.asList(new Integer[][]{{2, 1}, {3, 2}, {2, 3}, {1, 2}});
  }

  public DimensionComputationFaultTolerantTest(int applicationWindowCount, int checkpointWindowCount)
  {
    super(applicationWindowCount, checkpointWindowCount);
  }

  @Test
  public void test()
  {
    try {
      runApplication();
    } catch (Exception e) {
      Assert.assertFalse(e.getMessage(), true);
    }
    logger.info("Test for applicationWindowCount: {}, checkpointWindowCount: {} done.==========================", applicationWindowCount, checkpointWindowCount);
  }

  public void runApplication() throws Exception
  {
    Configuration conf = new Configuration(false);

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    tupleCount = 0;
    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    long endTime = System.currentTimeMillis() + atMostRunTime;
    while (tupleCount < tupleSize && System.currentTimeMillis() < endTime) {
      try {
        Thread.sleep(500);
      } catch (Exception e) {
        //ignore
      }
    }
    lc.shutdown();
    
    //the second setup would be failed if throw out exception
    Assert.assertTrue("Expect call setup() twice, Actually called " + setupTimes + " times",  setupTimes == 2);
  }
}
