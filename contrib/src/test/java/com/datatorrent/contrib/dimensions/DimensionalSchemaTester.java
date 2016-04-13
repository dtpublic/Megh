/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Sink;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.StoreFSTestWatcher;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.datatorrent.lib.util.time.WindowUtils;

public class DimensionalSchemaTester
{
  public static final String FIELD_responseDelayMillis = "\"responseDelayMillis\":";
  @Rule
  public TestInfo testMeta = new StoreFSTestWatcher();

  public CountAndLastTupleTestSink<String> resultSink = new CountAndLastTupleTestSink<String>();

  @Test
  public void testResponseDelayMillis()
  {
    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.queryResult.setSink((Sink)resultSink);

    //AttributeMap attributeMap = new StreamContext(id);
    final OperatorContext context = new com.datatorrent.stram.engine.OperatorContext(1, null, null);
    store.setup(context);

    long windowId = 1;
    store.beginWindow(windowId);
    doSchemaQuery(store);
    try {
      Thread.sleep(100);
    } catch (Exception e) {
      // ignore
    }
    store.endWindow();

    //get result from result sink
    String result = (String)resultSink.tuple;
    int offset = result.indexOf(FIELD_responseDelayMillis);
    Assert.assertTrue(offset > 0);
    String subResult = result.substring(offset);
    offset = subResult.indexOf(",");
    int delayMillis = Integer.valueOf(subResult.substring(FIELD_responseDelayMillis.length(), offset));
    Assert.assertTrue(delayMillis == WindowUtils.getAppWindowDurationMs(context));
  }

  protected void doSchemaQuery(AppDataSingleSchemaDimensionStoreHDHT store)
  {
    store.query.put("{\"id\":1122, \"type\":\"schemaQuery\"}");
  }
}
