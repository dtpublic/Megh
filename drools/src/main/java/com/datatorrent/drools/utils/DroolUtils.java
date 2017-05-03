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
package com.datatorrent.drools.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DroolUtils
{
  /**
   * Adds the knowledge jars to classpath of drools operator. knowledge jar should be available on hdfs or hadoop support file systems.
   * Provide path name with url e.g. hdfs://10.217.1.1:8020/user/test/rules
   * @param knowledge jar Path
   * @throws IOException
   */
  public static void addKjarToClasspath(String kjarPath) throws IOException
  {
    File localRulesDir = copyKjartoLocalFS(kjarPath);
    String[] kjarFiles = localRulesDir.list();
    URL[] urls = new URL[kjarFiles.length];
    int i = 0;
    for (String kjarFilePath : kjarFiles) {
      urls[i++] = new File(localRulesDir, kjarFilePath).toURI().toURL();
    }
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URLClassLoader urlClassLoader = URLClassLoader.newInstance(urls, loader);
    Thread.currentThread().setContextClassLoader(urlClassLoader);
  }

  private static File copyKjartoLocalFS(String kjarPath) throws IOException
  {
    FileSystem fs = getFSInstance(new Path(kjarPath));
    File localRulesDir = createTempLocalRulesDir();

    FileStatus rules = fs.getFileStatus(new Path(kjarPath));
    if (rules.isDirectory()) {
      for (FileStatus ruleFile : fs.listStatus(new Path(kjarPath))) {
        if (ruleFile.isFile()) {
          File localRulesFile = new File(localRulesDir, ruleFile.getPath().getName());
          fs.copyToLocalFile(false, ruleFile.getPath(), new Path(localRulesFile.getAbsolutePath()), true);
        }
      }
    } else {
      File localRulesFile = new File(localRulesDir, rules.getPath().getName());
      fs.copyToLocalFile(false, rules.getPath(), new Path(localRulesFile.getAbsolutePath()), true);
    }

    return localRulesDir;
  }

  private static File createTempLocalRulesDir() throws IOException
  {
    File localRulesDir = File.createTempFile("drools-rules-", "");
    localRulesDir.delete();
    localRulesDir.mkdir();
    return localRulesDir;
  }

  /**
   * Get {@link FileSystem} instance of file system having rules
   * @param rulesFile
   * @return fileSystem
   * @throws IOException
   */
  public static FileSystem getFSInstance(Path rulesFile) throws IOException
  {
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.newInstance(rulesFile.toUri(), configuration);
    return fs;
  }
}
