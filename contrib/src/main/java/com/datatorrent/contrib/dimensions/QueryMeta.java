/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.dimensions;

import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;

import java.util.List;
import java.util.Map;

/**
 * This class is used to hold meta data required to process data queries. This class is
 * utilized by the {@link DimensionsQueryExecutor} and {@link DimensionsQueueManager} classes.
 */
public class QueryMeta
{
  /**
   * Each entry in this list represents the {@link HDSQuery}s that are issued for a particular
   * time bucket. Each {@link Map} for each timebucket is a map from an {@link IncrementalAggregator} name
   * to the {@link HDSQuery} issued for that {@link IncrementalAggregator}. The {@link HDSQuery}s at a particular
   * index in this list correspond with the {@link EventKey}s stored at the same index in the eventKeys list.
   */
  private List<Map<String, HDSQuery>> hdsQueries;
  /**
   * Each entry in this list represents the {@link EventKey}s for queries issued for a particular time bucket. Each {@link Map}
   * for each timebucket is a map from an {@link IncrementalAggregator} name to the {@link EventKey} used to issue
   * the {@link HDSQuery} for that {@link IncrementalAggregator}.
   */
  private List<Map<String, EventKey>> eventKeys;

  /**
   * Creates a {@link QueryMeta} object.
   */
  public QueryMeta()
  {
    //Do nothing.
  }

  /**
   * Returns the hdsQueries used to retrieve data for a particular data query.
   * @return The hdsQueries used to retrieve data for a particular data query.
   */
  public List<Map<String, HDSQuery>> getHdsQueries()
  {
    return hdsQueries;
  }

  /**
   * Sets the hdsQueries used to retrieve data for a particular data query.
   * @param hdsQueries The hdsQueries used to retrieve data for a particular data query.
   */
  public void setHdsQueries(List<Map<String, HDSQuery>> hdsQueries)
  {
    this.hdsQueries = hdsQueries;
  }

  /**
   *
   * @return the event keys
   */
  public List<Map<String, EventKey>> getEventKeys()
  {
    return eventKeys;
  }

  /**
   * @param eventKeys event keys to set
   */
  public void setEventKeys(List<Map<String, EventKey>> eventKeys)
  {
    this.eventKeys = eventKeys;
  }

}
