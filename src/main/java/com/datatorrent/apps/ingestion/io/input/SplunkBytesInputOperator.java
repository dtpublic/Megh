/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.ingestion.io.input;

import java.util.Iterator;

import javax.validation.constraints.NotNull;

import com.splunk.Event;
import com.splunk.MultiResultsReaderXml;
import com.splunk.SearchResults;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.apps.ingestion.io.BandwidthLimitingInputOperator;
import com.datatorrent.apps.ingestion.lib.BandwidthManager;
import com.datatorrent.contrib.splunk.AbstractSplunkInputOperator;

/**
 * Concrete implementation of Splunk input operator
 */
public class SplunkBytesInputOperator extends AbstractSplunkInputOperator<byte[]> implements BandwidthLimitingInputOperator
{
  @NotNull
  private String query = "search * | head 100";
  private BandwidthManager bandwidthManager;
  private transient Iterator<SearchResults> resultsIterator;
  private transient Event currentEvent;

  public SplunkBytesInputOperator()
  {
    bandwidthManager = new BandwidthManager();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    bandwidthManager.setup(context);
  }

  @Override
  public byte[] getTuple(String value)
  {
    return value.getBytes();
  }

  @Override
  public String queryToRetrieveData()
  {
    return query;
  }

  /**
   * This executes the search query to retrieve result from splunk. It then converts each event's value into tuple and
   * emit that into output port.
   */
  @Override
  public void emitTuples()
  {
    if (currentEvent != null) {
      if(!emitTuple(currentEvent)) {
        return;
      }
    }
    try {
      if (resultsIterator == null) {
        exportSearch = store.getService().export(queryToRetrieveData(), exportArgs);
        multiResultsReader = new MultiResultsReaderXml(exportSearch);
        resultsIterator = multiResultsReader.iterator();
      }
      while (resultsIterator.hasNext()) {
        for (Event event : resultsIterator.next()) {
          currentEvent = event;
          if (!emitTuple(currentEvent)) {
            return;
          }
        }
      }
      resultsIterator = null;
      multiResultsReader.close();
    } catch (Exception e) {
      store.disconnect();
      throw new RuntimeException(String.format("Error while running query: %s", query), e);
    }
  }

  private boolean emitTuple(Event event)
  {
    for (String key : event.keySet()) {
      if (key.contains("raw")) {
        byte[] tuple = getTuple(event.get(key));
        if (bandwidthManager.canConsumeBandwidth(tuple.length)) {
          outputPort.emit(tuple);
          bandwidthManager.consumeBandwidth(tuple.length);
        } else {
          return false;
        }
      }
    }
    return true;
  }

  /*
   * Query to retrieve data from Splunk.
   */
  public String getQuery()
  {
    return query;
  }

  public void setQuery(String query)
  {
    this.query = query;
  }

  @Override
  public BandwidthManager getBandwidthManager()
  {
    return bandwidthManager;
  }

  public void setBandwidthManager(BandwidthManager bandwidthManager)
  {
    this.bandwidthManager = bandwidthManager;
  }
}
