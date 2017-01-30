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
package com.datatorrent.lib.appdata.dimensions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;

import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class DimensionsComputationCompositeAggregatorSimpleTest extends
    DimensionsComputationFlexibleSingleSchemaPOJOTest
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationCompositeAggregatorSimpleTest.class);

  @Before
  public void setup()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }


  @Test
  public void complexOutputTest()
  {
    AdInfo ai = createTestAdInfoEvent1();

    DimensionsComputationFlexibleSingleSchemaPOJO dcss =
        createDimensionsComputationOperator("adsGenericEventSimpleTopBottom.json");

    CollectorTestSink<Aggregate> sink = new CollectorTestSink<Aggregate>();
    TestUtils.setSink(dcss.output, sink);

    dcss.setup(null);
    dcss.beginWindow(0L);
    dcss.input.put(ai);
    dcss.endWindow();

    Assert.assertEquals(4, sink.collectedTuples.size());
  }

}
