package com.datatorrent.apps.ingestion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
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

public class ApplicationCompressionTest
{
  private static final String OUT_FILENAME = "dataFile.txt";
  private static final String FILE_DATA = "Test data for lzo compression tests.";

  public static class TestMeta extends TestWatcher
  {
    String dataDirectory;
    String baseDirectory;
    String outputDirectory;

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

  @Test(expected = RuntimeException.class)
  public void testLzoCompression() throws Exception
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
    conf.set("dt.application.Ingestion.compress", "true");
    conf.set("dt.application.Ingestion.compress.type", "lzo");
    conf.set("dt.application.Ingestion.compress.lzo.className", "");
    createFile();
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);
    lc.runAsync();

    Thread.sleep(10000);
    lc.shutdown();
  }

  @Test
  public void testGzipCompression() throws Exception
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
    conf.set("dt.operator.BlockReader.prop.uri",  "file://" + new File(testMeta.dataDirectory).getAbsolutePath());
    conf.set("dt.output.protocol", "file");
    conf.set("dt.application.Ingestion.attr.CHECKPOINT_WINDOW_COUNT", "10");
    conf.set("dt.application.Ingestion.attr.APPLICATION_PATH", testMeta.baseDirectory);
    conf.set("dt.application.Ingestion.attr.DEBUG", "false");
    conf.set("dt.application.Ingestion.compress", "true");
    conf.set("dt.application.Ingestion.compress.type", "gzip");
    createFile();
    lma.prepareDAG(new Application(), conf);
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
    String inputName = inpDir.getName();

    File compressedFile = new File(testMeta.outputDirectory + File.separator + inputName + File.separator + OUT_FILENAME + ".gz");
    GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(compressedFile));
    BufferedReader br = new BufferedReader(new InputStreamReader(gzipInputStream));
    try {
      Assert.assertEquals("Invalid decompressed data", FILE_DATA, br.readLine());
    } finally {
      br.close();
      gzipInputStream.close();
    }

    FileUtils.deleteDirectory(new File("target/com.datatorrent.stram.StramLocalCluster"));
    fs.close();

  }

  private void createFile()
  {
    try {
      File txtFile = new File(testMeta.dataDirectory + File.separator + OUT_FILENAME);
      FileUtils.write(txtFile, FILE_DATA);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationCompressionTest.class);

}