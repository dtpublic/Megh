/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.apps.ingestion;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

public class SubApplicationTest
{
  public static class TestMeta extends TestWatcher
  {
    String dataDirectory;
    String baseDirectory;
    String recoveryDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.dataDirectory = "src/test/resources/sample";
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
      this.recoveryDirectory = baseDirectory + "/recovery";
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      }
      catch (IOException e) {
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
    conf.set("dt.operator.FileSplitter.prop.directory", testMeta.dataDirectory);
    conf.set("dt.operator.FileSplitter.prop.scanner.filePatternRegexp", ".*?\\.txt");
    conf.set("dt.operator.FileSplitter.prop.idempotentStorageManager.recoveryPath", testMeta.recoveryDirectory);

    conf.set("dt.operator.BlockReader.prop.maxReaders", "1");
    conf.set("dt.operator.BlockReader.prop.directory", testMeta.dataDirectory);

    conf.set("dt.operator.BlockWriter.prop.filePath", "blocks");
    conf.set("dt.operator.BlockWriter.port.ftp.attr.PARTITION_PARALLEL", "true");
    conf.set("dt.operator.BlockWriter.port.blockMetadataInput.attr.PARTITION_PARALLEL", "true");

    DAG dag = lma.prepareDAG(new SubApplication(), conf);
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    String writerPath = dag.getValue(DAG.APPLICATION_PATH) + "/blocks";
    long now = System.currentTimeMillis();
    Path outDir = new Path(writerPath);
    FileSystem fs = FileSystem.newInstance(outDir.toUri(), new Configuration());
    while (!fs.exists(outDir) && System.currentTimeMillis() - now < 60000) {
      Thread.sleep(500);
      LOG.debug("Waiting for {}", outDir);
    }

    Thread.sleep(10000);
    lc.shutdown();

    Assert.assertTrue("output dir does not exist", fs.exists(outDir));

    FileStatus[] statuses = fs.listStatus(outDir);
    Assert.assertTrue("block file does not exist", statuses.length > 0 && fs.isFile(statuses[0].getPath()));
   FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
  }

  private static final Logger LOG = LoggerFactory.getLogger(SubApplicationTest.class);
}