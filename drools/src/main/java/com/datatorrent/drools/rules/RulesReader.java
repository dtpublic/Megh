/**
 * Copyright (c) 2017 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.drools.rules;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Evolving
public interface RulesReader
{

  /**
   * Load rules files from directory
   * @param rulesDir
   * @return kieFileSystem
   * @throws IOException
   */
  void loadRulesFromDirectory(String rulesDir) throws IOException;

  /**
   * Load rules files from list of files
   * @param ruleFiles
   * @return kieFileSystem
   * @throws IOException
   */
  void loadRulesFromFiles(String[] ruleFiles) throws IOException;

}
