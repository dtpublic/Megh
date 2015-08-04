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

import com.datatorrent.contrib.splunk.AbstractSplunkInputOperator;
import javax.validation.constraints.NotNull;

/**
 * Concrete implementation of Splunk input operator
 */
public class SplunkBytesInputOperator  extends AbstractSplunkInputOperator<byte[]>
{
  @NotNull
  private String query = "search * | head 100";

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
}
