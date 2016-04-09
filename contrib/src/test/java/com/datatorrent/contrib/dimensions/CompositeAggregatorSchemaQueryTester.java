/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
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
