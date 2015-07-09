/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.LocalMode;
import com.datatorrent.apps.ingestion.util.IngestionTestUtils;
import com.google.common.collect.Sets;

/**
 * Test the DAG declaration in local mode.
 *
 * @author Yogi/Sandeep
 */
public class ApplicationTest
{
  public static class TestMeta extends TestWatcher
  {
    public String dataDirectory;
    public String baseDirectory;
    public String outputDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
      this.outputDirectory = baseDirectory + "/output";
      this.dataDirectory = baseDirectory + "/data";
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAGContext.APPLICATION_PATH, testMeta.baseDirectory);

    conf.set("dt.operator.FileSplitter.prop.scanner.files", "file://" + new File(testMeta.dataDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.filePatternRegularExp", ".*?\\.txt");
    conf.set("dt.operator.FileMerger.prop.filePath", "file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.scanIntervalMillis", "10000");
    conf.set("dt.operator.BlockReader.prop.scheme", "file");
    conf.set("dt.output.protocol", "file");
    conf.set("dt.application.Ingestion.attr.CHECKPOINT_WINDOW_COUNT", "10");
    conf.set("dt.application.Ingestion.attr.APPLICATION_PATH", testMeta.baseDirectory);
    conf.set("dt.application.Ingestion.attr.DEBUG", "false");
    createFiles(testMeta.dataDirectory, 2, 2);
    lma.prepareDAG(new Application(), conf);
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    long now = System.currentTimeMillis();

    Path outDir = new Path("file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(10000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));

    Path inpDir = new Path("file://" + new File(testMeta.dataDirectory).getAbsolutePath());
    Assert.assertTrue("Input is not inside output correctly", IngestionTestUtils.compareInputInsideOutput(inpDir, outDir));

    FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
    fs.close();
  }
  
  @Test
  public void testApplicationCaseInsensitive() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAGContext.APPLICATION_PATH, testMeta.baseDirectory);

    conf.set("dt.operator.FileSplitter.prop.scanner.files", "FILE://" + new File(testMeta.dataDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.filePatternRegularExp", ".*?\\.txt");
    conf.set("dt.operator.FileMerger.prop.filePath", "FILE://" + new File(testMeta.outputDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.scanIntervalMillis", "10000");
    conf.set("dt.operator.BlockReader.prop.scheme", "FILE");
    conf.set("dt.output.protocol", "FILE");
    conf.set("dt.application.Ingestion.attr.CHECKPOINT_WINDOW_COUNT", "10");
    conf.set("dt.application.Ingestion.attr.APPLICATION_PATH", testMeta.baseDirectory);
    conf.set("dt.application.Ingestion.attr.DEBUG", "false");
    createFiles(testMeta.dataDirectory, 2, 2);
    lma.prepareDAG(new Application(), conf);
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    long now = System.currentTimeMillis();

    Path outDir = new Path("file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(10000);
    lc.shutdown();
    Assert.assertTrue("output dir does not exist", fs.exists(outDir));

    Path inpDir = new Path("file://" + new File(testMeta.dataDirectory).getAbsolutePath());
    Assert.assertTrue("Input is not inside output correctly", IngestionTestUtils.compareInputInsideOutput(inpDir, outDir));

    FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
    fs.close();
  }

  @Test
  public void testApplicationSummaryLogs() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAGContext.APPLICATION_PATH, testMeta.baseDirectory);

    conf.set("dt.operator.FileSplitter.prop.scanner.files", "file://" + new File(testMeta.dataDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.filePatternRegularExp", ".*?\\.txt");
    conf.set("dt.operator.FileMerger.prop.filePath", "file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    conf.set("dt.operator.FileSplitter.prop.scanner.scanIntervalMillis", "10000");
    conf.set("dt.operator.BlockReader.prop.scheme", "file");
    conf.set("dt.output.protocol", "file");
    conf.set("dt.application.Ingestion.attr.CHECKPOINT_WINDOW_COUNT", "10");
    conf.set("dt.application.Ingestion.attr.APPLICATION_PATH", testMeta.baseDirectory);
    conf.set("dt.application.Ingestion.attr.DEBUG", "false");
    createFiles(testMeta.dataDirectory, 2, 2);
    lma.prepareDAG(new Application(), conf);
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    long now = System.currentTimeMillis();

    Path outDir = new Path("file://" + new File(testMeta.outputDirectory).getAbsolutePath());
    
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 20000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }
    Thread.sleep(10000);
    lc.shutdown();
    
    Path summaryDir = new Path(testMeta.baseDirectory, "summary");
    Assert.assertTrue("Summary dir does not exist", fs.exists(summaryDir));
    Assert.assertTrue("summary info logs does not exist", fs.exists(new Path(summaryDir, "summary_info.log")));
    Assert.assertTrue("successful files logs does not exist", fs.exists(new Path(summaryDir, "successful_files.log")));
    
    String actual = FileUtils.readFileToString(new File(summaryDir.toString(),"successful_files.log"));
    Set<String> actualSet = new HashSet<String>(Arrays.asList(actual.split("\n")));
    
    Path dataPath = new Path(new File(testMeta.baseDirectory).getAbsolutePath(),"data");
    Set<String> expectedSet = new HashSet<String>();
    expectedSet.add(new Path(dataPath,"file1.txt").toString());
    expectedSet.add(new Path(dataPath, "file0.txt").toString());
    Assert.assertEquals("Sucessful files list not matching", expectedSet, actualSet);

    FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
    fs.close();
  }


  private void createFiles(String basePath, int numFiles, int numLines)
  {
    try {
      HashSet<String> allLines = Sets.newHashSet();
      for (int file = 0; file < numFiles; file++) {
        HashSet<String> lines = Sets.newHashSet();
        for (int line = 0; line < numLines; line++) {
          lines.add("f" + file + "l" + line);
        }
        allLines.addAll(lines);
        File txtFile = new File(basePath, "/file" + file + ".txt");
        FileUtils.write(txtFile, StringUtils.join(lines, '\n') + '\n');
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
}
