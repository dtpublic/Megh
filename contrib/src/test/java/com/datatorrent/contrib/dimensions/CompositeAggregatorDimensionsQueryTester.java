/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;
import org.apache.commons.lang3.mutable.MutableLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.FieldsAggregatable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.netlet.util.Slice;

public class CompositeAggregatorDimensionsQueryTester extends CompositeDimensionComputationTester
{
  @Override
  public void aggregationTest()
  {
  }
  
  @Test
  public void queryDataTest()
  {
    testCompositeAggregation();
  }

  @Override
  protected void doBeforeEndWindow(long windowId)
  {
    if (windowId != 2) {
      return;
    }
    
    DataResultDimensional drd = doQuery();
    Assert.assertEquals(1, drd.getValues().size());
  }
  
  public DataResultDimensional doQuery()
  {
    List<Map<String, HDSQuery>> hdsQueries = Lists.newArrayList();
    List<Map<String, EventKey>> eventKeys = Lists.newArrayList();

    DimensionsQueryExecutor dqe = new DimensionsQueryExecutor(store, store.schemaRegistry);
    
    
    final String aggregatorName = "TOPN-SUM-10_location";
    final int aggregatorId = store.getAggregatorRegistry().getTopBottomAggregatorNameToID().get(aggregatorName);
    
    EventKey eventKey = null;
    for (EventKey ek : totalEventKeys) {
      if (ek.getAggregatorID() == aggregatorId) {
        eventKey = ek;
        break;
      }
    }
    
    issueHDSQuery(store, eventKey);
    
    Map<String, HDSQuery> aggregatorToQuery = Maps.newHashMap();
    aggregatorToQuery.put(aggregatorName, store.getQueries().values().iterator().next());
    hdsQueries.add(aggregatorToQuery);

    Map<String, EventKey> aggregatorToEventKey = Maps.newHashMap();
    aggregatorToEventKey.put(aggregatorName, eventKey);
    eventKeys.add(aggregatorToEventKey);

    QueryMeta queryMeta = new QueryMeta();
    queryMeta.setHdsQueries(hdsQueries);
    queryMeta.setEventKeys(eventKeys);

    long currentTime = 0L;
    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(0);
    Map<String, Set<Object>> keyToValues = Maps.newHashMap();
    keyToValues.put("publisher", Sets.newHashSet());
    //keyToValues.put("advertiser", Sets.newHashSet());

    Map<String, Set<String>> fieldToAggregators = Maps.newHashMap();
    fieldToAggregators.put("impressions", Sets.newHashSet(aggregatorName));
    fieldToAggregators.put("cost", Sets.newHashSet(aggregatorName));

    FieldsAggregatable fieldsAggregatable = new FieldsAggregatable(fieldToAggregators);
    
    DataQueryDimensional query = new DataQueryDimensional("1",
        DataQueryDimensional.TYPE,
        currentTime,
        currentTime,
        new CustomTimeBucket(TimeBucket.MINUTE),
        fdKey,
        keyToValues,
        fieldsAggregatable,
        true);
    
    return (DataResultDimensional)dqe.executeQuery(query, queryMeta, new MutableLong(1L));
  }
  
  
  public static void issueHDSQuery(DimensionsStoreHDHT store, EventKey eventKey)
  {
    LOG.debug("Issued QUERY");
    Slice key = new Slice(store.getEventKeyBytesGAE(eventKey));
    HDSQuery hdsQuery = new HDSQuery();
    hdsQuery.bucketKey = AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID;
    hdsQuery.key = key;
    store.addQuery(hdsQuery);
  }

  
  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueryExecutorTest.class);
}
