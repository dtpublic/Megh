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

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.StoreFSTestWatcher;
import com.datatorrent.contrib.dimensions.CompositeDimensionComputationTester.TestStoreHDHT;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
import com.datatorrent.lib.appdata.schemas.SchemaResultSerializer;
import com.datatorrent.lib.util.TestUtils.TestInfo;

public class CompositeAggregatorSchemaQueryTester
{
  @Rule
  public TestInfo testMeta = new StoreFSTestWatcher();

  @Test
  public void querySchemaTest()
  {
    CompositeDimensionComputationTester computationTest = new CompositeDimensionComputationTester();
    //testCompositeAggregation();
    TestStoreHDHT store = computationTest.setupStore(testMeta);
    querySchema(store);
  }
  
  public void querySchema(TestStoreHDHT store)
  {
    SchemaResult schemaResult =
        store.getSchemaProcessor().getQueryExecutor().executeQuery(new SchemaQuery("1"), null, null);
    SchemaResultSerializer serializer = new SchemaResultSerializer();
    String serialized = serializer.serialize(schemaResult, null);
    LOG.info(serialized);
  }


  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueryExecutorTest.class);
}
