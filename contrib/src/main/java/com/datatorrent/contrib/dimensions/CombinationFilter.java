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

import java.util.Map;
import java.util.Set;

/**
 * This interface provides method to filter the input keyToValues
 * 
 * @since 3.3.0
 */
public interface CombinationFilter
{
  /**
   * filter the input keyToValues
   * @param keyToValues the source keyToValues to be filtered.
   * @return the filtered keyToValues map
   */
  public Map<String, Set<Object>> filter(Map<String, Set<Object>> keyToValues);
}
