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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.datatorrent.drools.utils.DroolUtils;

public class DroolUtilsTest
{
  public static class TestMeta extends TestWatcher
  {
    private String rulesDirectory;

    @Override
    protected void starting(Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.rulesDirectory = "target/" + className + "/" + methodName + "/rules";

      try {
        createRulesFiles();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    protected void finished(Description description)
    {
      FileUtils.deleteQuietly(new File("target/" + description.getClassName()));
    }

    private void createRulesFiles() throws IOException
    {
      File rulesDir = new File(rulesDirectory);
      rulesDir.mkdirs();
      JarOutputStream target = new JarOutputStream(new FileOutputStream(rulesDirectory + "/kjar.jar"));
      addkmoduleFile(target);
      target.close();
    }

    private void addkmoduleFile(JarOutputStream target) throws IOException
    {
      JarEntry entry = new JarEntry("src/main/resources/META-INF/kmodule.xml");
      target.putNextEntry(entry);

      byte[] buffer = "<kmodule xmlns=\"http://jboss.org/kie/6.0.0/kmodule\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"></kmodule>"
          .getBytes();
      target.write(buffer, 0, buffer.length);
      target.closeEntry();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void test() throws IOException
  {
    DroolUtils.addKjarToClasspath(testMeta.rulesDirectory);
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("META-INF/kmodule.xml").getFile());
    Assert.assertTrue(file.exists());
  }
}
