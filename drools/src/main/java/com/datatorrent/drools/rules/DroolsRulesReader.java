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
import java.io.InputStream;

import org.drools.core.io.impl.InputStreamResource;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.drools.utils.DroolUtils;

/**
 * Reads rule files (supported format drl, xls) from hdfs.
 *
 */
public class DroolsRulesReader implements RulesReader
{
  private static final Logger LOG = LoggerFactory.getLogger(DroolsRulesReader.class);
  static final String RESOURCES_ROOT = "src/main/resources/";

  @Override
  public void loadRulesFromDirectory(String rulesDir) throws IOException
  {
    Path rulesDirPath = new Path(rulesDir);
    FileSystem sourceFileSystem = DroolUtils.getFSInstance(rulesDirPath);

    loadRulesInKieFileSystem(sourceFileSystem, sourceFileSystem.listStatus(rulesDirPath));
  }

  @Override
  public void loadRulesFromFiles(String[] ruleFiles) throws IOException
  {
    FileSystem sourceFileSystem = DroolUtils.getFSInstance(new Path(ruleFiles[0]));
    FileStatus[] rulesfileStatus = new FileStatus[ruleFiles.length];
    int i = 0;
    for (String ruleFile : ruleFiles) {
      rulesfileStatus[i++] = sourceFileSystem.getFileStatus(new Path(ruleFile));
    }

    loadRulesInKieFileSystem(sourceFileSystem, rulesfileStatus);
  }

  /**
   * Load rules from given source {@link FileSystem} in {@link KieFileSystem}
   * @param kieFileSystem
   * @param sourceFileSystem
   * @param ruleFiles
   * @throws IOException
   */
  protected void loadRulesInKieFileSystem(FileSystem sourceFileSystem, FileStatus[] ruleFiles) throws IOException
  {
    KieServices kieServices = KieServices.Factory.get();
    KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
    for (FileStatus rulesFileStatus : ruleFiles) {
      if (rulesFileStatus.isFile()) {
        InputStream rulesFileStream = sourceFileSystem.open(rulesFileStatus.getPath());
        kieFileSystem.write(RESOURCES_ROOT + rulesFileStatus.getPath().getName(),
            new InputStreamResource(rulesFileStream));
      }
    }

    KieBuilder kieBuilder = kieServices.newKieBuilder(kieFileSystem).buildAll();
    validateRuleLoading(kieBuilder);
  }

  //validates if there was any errors while loading rules.
  private void validateRuleLoading(KieBuilder kieBuilder)
  {
    if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
      LOG.error("Error loading rules. \n" + kieBuilder.getResults().getMessages());
      throw new RuntimeException("Error loading rules.");
    }
  }
}
