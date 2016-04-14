/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;

import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.query.serde.DataQueryDimensionalDeserializerTest.DeserializerTestWatcher;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;

public class CompositeAggregatorQueryDeserialilzerTest
{
  @Rule
  public DeserializerTestWatcher deserializerTestWatcher = new DeserializerTestWatcher();
  
  @BeforeClass
  public static void setup()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }
  
  @Test
  public void testSimpleQueryDeserialize() throws Exception
  {
    DataQueryDimensional dqd = getDataQueryDimensional("compositeAggregatorDimensionalQuery.json");
    validateDataQueryDimensional(dqd);
    Assert.assertEquals(10, dqd.getLatestNumBuckets());
  }
  
  protected DataQueryDimensional getDataQueryDimensional(String jsonFile) throws Exception
  {
    DataQueryDimensionalDeserializer dqdd = new DataQueryDimensionalDeserializer();
    String json = SchemaUtils.jarResourceFileToString(jsonFile);

    return (DataQueryDimensional)dqdd.deserialize(json, DataQueryDimensional.class,
        deserializerTestWatcher.getSchemaRegistry());
  }
  
  protected void validateDataQueryDimensional(DataQueryDimensional dataQueryDimensional)
  {
    Assert.assertEquals("1", dataQueryDimensional.getId());
    Assert.assertEquals(TimeBucket.MINUTE, dataQueryDimensional.getTimeBucket());
    Assert.assertEquals(true, dataQueryDimensional.getIncompleteResultOK());
    Assert.assertEquals(Sets.newHashSet("publisher"), dataQueryDimensional.getKeyFields().getFields());
    
    //"impressions:TOPN:SUM:10:location", "cost:TOPN:SUM:10:location", "cost:BOTTOMN:20:location"
    Assert.assertEquals(Sets.newHashSet("impressions", "cost"),
        dataQueryDimensional.getFieldsAggregatable().getAggregatorToFields().get("TOPN-SUM-10_location"));
    Assert.assertEquals(Sets.newHashSet("cost"),
        dataQueryDimensional.getFieldsAggregatable().getAggregatorToFields().get("BOTTOMN-AVG-20_location"));
    Assert.assertEquals(Sets.newHashSet("time"),
        dataQueryDimensional.getFieldsAggregatable().getNonAggregatedFields().getFields());
  }

}
