/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.Result;

/**
 * This unifier merge the query result. If any one is not empty, output
 * non-empty. If all are empty, output empty. Note: there probably have multiple
 * quests, and multiple responses
 * 
 * @since 3.3.0
 */
public class DimensionQueryResultMergeUnifier extends BaseOperator implements Unifier<String>
{
  private static final transient Logger logger = LoggerFactory.getLogger(DimensionQueryResultMergeUnifier.class);
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  //id ==> list of data
  /**
   * id ==> empty tuple. currently, only one partition return the non-empty
   * result, all other partitions return empty result.
   */
  protected transient Map<String, String> idToEmptyTupleMap = Maps.newHashMap();

  protected transient Set<String> handledIds = Sets.newHashSet();

  @Override
  public void beginWindow(long windowId)
  {
    idToEmptyTupleMap.clear();
    handledIds.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endWindow()
  {
    //emit empty tuples
    for (String tuple : idToEmptyTupleMap.values()) {
      output.emit(tuple);
    }
  }

  @Override
  public void process(String tuple)
  {
    JSONObject jo = null;

    try {
      jo = new JSONObject(tuple);
      if (jo.getString(Result.FIELD_TYPE).equals(DataResultDimensional.TYPE)) {
        String id = jo.getString(Result.FIELD_ID);
        if (handledIds.contains(id)) {
          return;
        }

        JSONArray dataArray = jo.getJSONArray(Result.FIELD_DATA);
        boolean isEmpty = ((dataArray == null) || (dataArray.length() == 0));
        if (!isEmpty) {
          //send response directly
          output.emit(tuple);
          handledIds.add(id);
          if (idToEmptyTupleMap.containsKey(id)) {
            idToEmptyTupleMap.remove(id);
          }
        } else {
          Set<String> emptyTupleIds = idToEmptyTupleMap.keySet();
          if (!emptyTupleIds.contains(id)) {
            idToEmptyTupleMap.put(id, tuple);
          }
        }
      } else {
        logger.debug("Invalid type: {}, by pass.", jo.getString(Result.FIELD_TYPE));
        output.emit(tuple);
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

}
