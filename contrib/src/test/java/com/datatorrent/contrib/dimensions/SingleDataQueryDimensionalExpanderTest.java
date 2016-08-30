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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.dimensions.DimensionsDescriptor;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

public class SingleDataQueryDimensionalExpanderTest
{
  @Test
  public void simpleTest()
  {
    Map<String, Set<Object>> keyToValues = Maps.newHashMap();

    Set<Object> aValues = Sets.newHashSet();
    aValues.add(1L);
    keyToValues.put("a", aValues);

    Set<Object> bValues = Sets.newHashSet();
    bValues.add(2L);
    keyToValues.put("b", bValues);

    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("a", Type.LONG);
    fieldToType.put("b", Type.LONG);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);
    List<GPOMutable> gpos = SingleDataQueryDimensionalExpander.INSTANCE.createGPOs(keyToValues, fd);
    GPOMutable gpo = gpos.get(0);

    Assert.assertEquals(1, gpos.size());
    Assert.assertEquals(1L, gpo.getFieldLong("a"));
    Assert.assertEquals(2L, gpo.getFieldLong("b"));
  }

  @Test
  public void emptyFieldsDescriptorTest()
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    fieldToType.put(DimensionsDescriptor.DIMENSION_TIME, Type.STRING);
    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);

    Map<String, Set<Object>> keyToValues = Maps.newHashMap();
    List<GPOMutable> gpos = SingleDataQueryDimensionalExpander.INSTANCE.createGPOs(keyToValues, fd);

    Assert.assertEquals(1, gpos.size());
  }
}
