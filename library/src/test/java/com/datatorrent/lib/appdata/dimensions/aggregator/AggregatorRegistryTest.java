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
package com.datatorrent.lib.appdata.dimensions.aggregator;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorAverage;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorCount;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorSum;
import org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.OTFAggregator;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.lib.util.KryoCloneUtils;

public class AggregatorRegistryTest
{
  @Test
  public void serializationTest() throws Exception
  {
    KryoCloneUtils.cloneObject(new Kryo(), AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
  }

  @Test
  public void metaDataTest()
  {
    Map<String, IncrementalAggregator> nameToIncrementalAggregator = Maps.newHashMap();
    nameToIncrementalAggregator.put("SUM", new AggregatorSum());
    nameToIncrementalAggregator.put("COUNT", new AggregatorCount());

    Map<String, OTFAggregator> nameToOTFAggregator = Maps.newHashMap();
    nameToOTFAggregator.put("AVG", AggregatorAverage.INSTANCE);

    Map<String, Integer> nameToID = Maps.newHashMap();
    nameToID.put("SUM", 0);
    nameToID.put("COUNT", 1);

    AggregatorRegistry aggregatorRegistry = new AggregatorRegistry(nameToIncrementalAggregator, nameToOTFAggregator, nameToID);

    aggregatorRegistry.setup();

    Map<Class<? extends IncrementalAggregator>, String> classToStaticAggregator =
        aggregatorRegistry.getClassToIncrementalAggregatorName();

    Assert.assertEquals("Incorrect number of elements.", 2, classToStaticAggregator.size());
    Assert.assertEquals(classToStaticAggregator.get(AggregatorSum.class), "SUM");
    Assert.assertEquals(classToStaticAggregator.get(AggregatorCount.class), "COUNT");

    Assert.assertEquals(AggregatorAverage.class, nameToOTFAggregator.get("AVG").getClass());

    Map<String, List<String>> otfAggregatorToStaticAggregators =
        aggregatorRegistry.getOTFAggregatorToIncrementalAggregators();

    Assert.assertEquals("Only 1 OTF aggregator", 1, otfAggregatorToStaticAggregators.size());
    Assert.assertEquals(otfAggregatorToStaticAggregators.get("AVG"), Lists.newArrayList("SUM","COUNT"));
  }
}
