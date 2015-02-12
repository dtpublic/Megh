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
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.datatorrent.lib.io.fs.FileSplitter.FileMetadata;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;

public class IngestionFileSplitterRecursiveTest
{

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

      clean();

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

    private void clean()
    {
      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

    private void create()
    {
      try {
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

  @Rule
  public TestRecursiveFileSplitter testRecursiveMeta = new TestRecursiveFileSplitter();

  private static String DIR_1 = "dir1.1";
  private static String DIR_2 = "dir2.1/d22";
  private static String DIR_3 = "dir31/dire32/dir33";
  private static String DIR_4 = "d4";

  @Test
  public void testRecursiveScanSanity()
  {
    int numFiles = 2;
    int numLines = 5;
    int lineSize = 5;
    int numDirs = 1;
    mkdir(testRecursiveMeta.dataDirectory + DIR_1);
    createFiles(testRecursiveMeta.dataDirectory + DIR_1, 2, 5); // 2 files of 5 ( of size 24 byes) lines each

    int blockSize = 10;
    testRecursiveMeta.fileSplitter.setBlockSize(new Long(blockSize));
    testRecursiveMeta.fileSplitter.beginWindow(1);
    testRecursiveMeta.fileSplitter.emitTuples();

    int noOfBlocks = numFiles * (int) Math.ceil(((1.0 * numLines * lineSize) / blockSize));
    Assert.assertEquals("Blocks", noOfBlocks, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", numDirs + numFiles, testRecursiveMeta.fileMetadataSink.collectedTuples.size());

    @SuppressWarnings({ "unchecked", "rawtypes" })
    ArrayList<FileMetadata> fileList = (ArrayList) testRecursiveMeta.fileMetadataSink.collectedTuples;
    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "dir1.1/file0.txt"));
    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "dir1.1/file1.txt"));
    Assert.assertEquals("Missing entry:", false, hasFileEntry(fileList, "dir1.1/file2.txt"));
    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "dir1.1")); // No block entry for directory

    @SuppressWarnings({ "unchecked", "rawtypes" })
    ArrayList<FileBlockMetadata> blockList = (ArrayList) testRecursiveMeta.blockMetadataSink.collectedTuples;
    Assert.assertEquals("Missing entry:", false, hasBlockEntry(blockList, "dir1.1"));
    Assert.assertEquals("Missing entry:", true, hasBlockEntry(blockList, "dir1.1/file0.txt"));
  }

  @Test
  public void testRecursiveScanZeroFileSize()
  {
    mkdir(testRecursiveMeta.dataDirectory + DIR_1);
    try {
      FileUtils.touch(new File(testRecursiveMeta.dataDirectory + DIR_1 + "/zero.txt"));
    } catch (IOException e) {
      throw new RuntimeException("Unable to create zero size file.");
    }
    int blockSize = 10;
    testRecursiveMeta.fileSplitter.setBlockSize(new Long(blockSize));
    testRecursiveMeta.fileSplitter.beginWindow(1);
    testRecursiveMeta.fileSplitter.emitTuples();

    Assert.assertEquals("Blocks", 0, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 2, testRecursiveMeta.fileMetadataSink.collectedTuples.size());

    @SuppressWarnings({ "unchecked", "rawtypes" })
    ArrayList<FileMetadata> list = (ArrayList) testRecursiveMeta.fileMetadataSink.collectedTuples;
    Assert.assertEquals("Directory entry mismatch", -1, list.get(0).getFileLength());
    Assert.assertEquals("Directory name mismatch", DIR_1, list.get(0).getFileName());
    Assert.assertEquals("Zero file entry mismatch", 0, list.get(1).getFileLength());
    Assert.assertEquals("Directory name mismatch", DIR_1 + "/zero.txt", list.get(1).getFileName());

    Assert.assertEquals("Missing entry:", true, hasFileEntry(list, "dir1.1/zero.txt"));
    Assert.assertEquals("Missing entry:", true, hasFileEntry(list, "dir1.1"));
  }

  private boolean hasFileEntry(ArrayList<FileMetadata> list, String fileName)
  {
    for (FileMetadata fm : list) {
      if (fm.getFileName().equalsIgnoreCase(fileName)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasBlockEntry(ArrayList<FileBlockMetadata> list, String fileName)
  {
    for (FileBlockMetadata fm : list) {
      System.out.println(fm.getFilePath());
      if (fm.getFilePath().endsWith(fileName)) {
        return true;
      }
    }
    return false;
  }

  public void mkdir(String dir)
  {
    new File(dir).mkdirs();
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
        File created = new File(basePath, "/file" + file + ".txt");
        testRecursiveMeta.filePaths.add("file:" + created.getAbsolutePath());
        FileUtils.write(created, StringUtils.join(lines, '\n') + '\n');
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
