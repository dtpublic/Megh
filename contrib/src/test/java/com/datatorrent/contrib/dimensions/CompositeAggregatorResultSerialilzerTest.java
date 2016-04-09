/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.appdata.query.serde.DataResultDimensionalSerializer;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;

public class CompositeAggregatorResultSerialilzerTest extends CompositeAggregatorDimensionsQueryTester
{
  @Override
  public void queryDataTest()
  {
  }
  
  
  @Test
  public void queryResultSerializeTest()
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
    
    DataResultDimensionalSerializer serializer = new DataResultDimensionalSerializer();
    String serializedString = serializer.serialize(drd, new ResultFormatter());
    verifySerializedString(serializedString);
  }
  
  protected final String expectedString =
      "{\"id\":\"1\",\"data\":[{\"impressions:TOPN-SUM-10_location\":\"[{ON:52},{WA:51},{BC:53},{CA:50}]\"," +
      "\"cost:TOPN-SUM-10_location\":\"[{ON:102.0},{WA:101.0},{BC:103.0},{CA:100.0}]\"}]}";
  protected void verifySerializedString(String serializedString)
  {
    Assert.assertEquals(expectedString, serializedString);
  }
}
