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
