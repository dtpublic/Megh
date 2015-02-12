package com.datatorrent.apps.ingestion.io.input;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;

public class IngestionFileSplitterTest
{
  public static class TestBaseFileSplitter extends TestWatcher
  {
    public String dataDirectory = null;
    public String recoveryDirectory = null;

    public IngestionFileSplitter fileSplitter;
    public CollectorTestSink<Object> fileMetadataSink;
    public CollectorTestSink<Object> blockMetadataSink;
    public Set<String> filePaths = Sets.newHashSet();

    @Override
    protected void starting(org.junit.runner.Description description)
    {

      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dataDirectory = "target/" + className + "/" + "data/";
      this.recoveryDirectory = "target/" + className + "/" + "recovery/";

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
        HashSet<String> allLines = Sets.newHashSet();
        for (int file = 0; file < 2; file++) {
          HashSet<String> lines = Sets.newHashSet();
          for (int line = 0; line < 5; line++) {
            lines.add("f" + file + "l" + line);
          }
          allLines.addAll(lines);
          File created = new File(this.dataDirectory, "file" + file + ".txt");
          filePaths.add("file:" + created.getAbsolutePath());
          FileUtils.write(created, StringUtils.join(lines, '\n'));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.fileSplitter = new IngestionFileSplitter();

      IngestionFileSplitter.RecursiveDirectoryScanner scanner = new IngestionFileSplitter.RecursiveDirectoryScanner();
      scanner.setFilePatternRegexp(".*[.]txt");
      fileSplitter.setScanner(scanner);
      fileSplitter.setDirectory(dataDirectory);
      fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
      fileSplitter.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, new Attribute.AttributeMap.DefaultAttributeMap()));

      fileMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.filesMetadataOutput.setSink(fileMetadataSink);

      blockMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.blocksMetadataOutput.setSink(blockMetadataSink);
    }

    @Override
    protected void finished(Description description)
    {
      // this.filePaths.clear();
      this.fileSplitter.teardown();
      try {
        FileUtils.deleteDirectory(new File(this.dataDirectory));
        FileUtils.deleteDirectory(new File(this.recoveryDirectory));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestBaseFileSplitter testMeta = new TestBaseFileSplitter();

  @Test
  public void testFileMetadata()
  {
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals("File metadata", 2, testMeta.fileMetadataSink.collectedTuples.size());
    for (Object fileMetada : testMeta.fileMetadataSink.collectedTuples) {
      FileSplitter.FileMetadata metadata = (FileSplitter.FileMetadata) fileMetada;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
      Assert.assertNotNull("name: ", metadata.getFileName());
    }
  }

  @Test
  public void testBlockMetadataNoSplit()
  {
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : testMeta.blockMetadataSink.collectedTuples) {
      BlockMetadata.FileBlockMetadata metadata = (BlockMetadata.FileBlockMetadata) blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
    }
  }

  @Test
  public void testBlockMetadataWithSplit()
  {
    int blockSize = 10;
    testMeta.fileSplitter.setBlockSize(new Long(blockSize));
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();

    int noOfBlocks = 0;
    for (int file = 0; file < 2; file++) {
      File testFile = new File(testMeta.dataDirectory, "file" + file + ".txt");
      noOfBlocks += (int) Math.ceil(testFile.length() / (blockSize * 1.0));
    }
    Assert.assertEquals("Blocks", noOfBlocks, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testIdempotency()
  {
    Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.DAGContext.APPLICATION_ID, "FileSplitterTest");
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(0, attributes);

    IdempotentStorageManager.FSIdempotentStorageManager fsIdempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    fsIdempotentStorageManager.setRecoveryPath(testMeta.recoveryDirectory);
    testMeta.fileSplitter.setIdempotentStorageManager(fsIdempotentStorageManager);

    testMeta.fileSplitter.setup(context);
    // will emit window 1 from data directory
    testFileMetadata();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitter.setup(context);
    testMeta.fileSplitter.beginWindow(1);
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : testMeta.blockMetadataSink.collectedTuples) {
      BlockMetadata.FileBlockMetadata metadata = (BlockMetadata.FileBlockMetadata) blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
    }
  }

  public static class TestRecursiveFileSplitter extends TestWatcher
  {
    public String dataDirectory = null;
    public String recoveryDirectory = null;

    public IngestionFileSplitter fileSplitter;
    public CollectorTestSink<Object> fileMetadataSink;
    public CollectorTestSink<Object> blockMetadataSink;
    public Set<String> filePaths = Sets.newHashSet();

    @Override
    protected void starting(org.junit.runner.Description description)
    {

      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dataDirectory = "target/" + className + "/" + "data/";
      this.recoveryDirectory = "target/" + className + "/" + "recovery/";

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
        HashSet<String> allLines = Sets.newHashSet();
        for (int file = 0; file < 2; file++) {
          HashSet<String> lines = Sets.newHashSet();
          for (int line = 0; line < 5; line++) {
            lines.add("f" + file + "l" + line);
          }
          allLines.addAll(lines);
          File created = new File(this.dataDirectory, "file" + file + ".txt");
          filePaths.add("file:" + created.getAbsolutePath());
          FileUtils.write(created, StringUtils.join(lines, '\n'));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      this.fileSplitter = new IngestionFileSplitter();

      IngestionFileSplitter.RecursiveDirectoryScanner scanner = new IngestionFileSplitter.RecursiveDirectoryScanner();
      scanner.setFilePatternRegexp(".*[.]txt");
      fileSplitter.setScanner(scanner);
      fileSplitter.setDirectory(dataDirectory);
      fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
      fileSplitter.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, new Attribute.AttributeMap.DefaultAttributeMap()));

      fileMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.filesMetadataOutput.setSink(fileMetadataSink);

      blockMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.blocksMetadataOutput.setSink(blockMetadataSink);
    }

    @Override
    protected void finished(Description description)
    {
       this.filePaths.clear();
      this.fileSplitter.teardown();
      try {
        FileUtils.deleteDirectory(new File(this.dataDirectory));
        FileUtils.deleteDirectory(new File(this.recoveryDirectory));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
