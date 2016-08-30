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

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.Result;

/**
 * This is a QueryResult unifier for AbstractAppDataDimensionStoreHDHT for port queryResult.
 *
 * The unifier filters out all the data which are having empty query result data.
 *
 * This is specially useful when Store is partitioned and Queries goes to all the stores but
 * only one store is going to hold the data for the actual results.
 *
 * @since 3.2.0
 */
public class DimensionStoreHDHTNonEmptyQueryResultUnifier extends BaseOperator implements Unifier<String>
{

  /**
   * Output port that emits only non-empty dataResults.
   */
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void process(String tuple)
  {
    JSONObject jo = null;

    try {
      jo = new JSONObject(tuple);
      if (jo.getString(Result.FIELD_TYPE).equals(DataResultDimensional.TYPE)) {
        JSONArray dataArray = jo.getJSONArray(Result.FIELD_DATA);
        if ((dataArray != null) && (dataArray.length() != 0)) {
          output.emit(tuple);
        }
      } else {
        output.emit(tuple);
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }
}
