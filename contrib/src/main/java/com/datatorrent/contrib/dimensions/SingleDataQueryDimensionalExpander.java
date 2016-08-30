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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensionalExpander;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;


/**
 *
 * @since 3.3.0
 */

public class SingleDataQueryDimensionalExpander implements DataQueryDimensionalExpander
{
  public static final SingleDataQueryDimensionalExpander INSTANCE = new SingleDataQueryDimensionalExpander();

  @Override
  public List<GPOMutable> createGPOs(Map<String, Set<Object>> keyToValues, FieldsDescriptor fd)
  {
    List<GPOMutable> keys = Lists.newArrayList();
    GPOMutable gpo = new GPOMutable(fd);
    keys.add(gpo);

    for (Map.Entry<String, Set<Object>> entry : keyToValues.entrySet()) {
      Preconditions.checkArgument(entry.getValue().size() == 1, "There should only be one value for the key.");
      gpo.setFieldGeneric(entry.getKey(), entry.getValue().iterator().next());
    }

    return keys;
  }
}
