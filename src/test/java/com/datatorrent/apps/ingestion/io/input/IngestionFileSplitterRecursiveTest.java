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
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;

public class IngestionFileSplitterRecursiveTest
{
//
//  public static class TestRecursiveFileSplitter extends TestWatcher
//  {
//    public String dataDirectory = null;
//    public String recoveryDirectory = null;
//
//    public IngestionFileSplitter fileSplitter;
//    public CollectorTestSink<Object> fileMetadataSink;
//    public CollectorTestSink<Object> blockMetadataSink;
//    public Set<String> filePaths = Sets.newHashSet();
//
//    @Override
//    protected void starting(org.junit.runner.Description description)
//    {
//      String className = description.getClassName();
//      this.dataDirectory = "target/" + className + "/" + "data/";
//      this.recoveryDirectory = "target/" + className + "/" + "recovery/";
//
//      clean();
//
//      this.fileSplitter = new IngestionFileSplitter();
//      RecursiveDirectoryScanner scanner = new RecursiveDirectoryScanner();
//      scanner.setFilePatternRegexp(".*[.]txt");
//
//      fileSplitter.setDirectory(dataDirectory);
//      fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
//      fileSplitter.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, new Attribute.AttributeMap.DefaultAttributeMap()));
//
//      ((RecursiveDirectoryScanner)fileSplitter.getScanner()).setRecursiveScan(true);
//
//      fileMetadataSink = new CollectorTestSink<Object>();
//      fileSplitter.filesMetadataOutput.setSink(fileMetadataSink);
//
//      blockMetadataSink = new CollectorTestSink<Object>();
//      fileSplitter.blocksMetadataOutput.setSink(blockMetadataSink);
//    }
//
//    private void clean()
//    {
//      try {
//        FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//
//    }
//
//    @Override
//    protected void finished(Description description)
//    {
//      this.filePaths.clear();
//      this.fileSplitter.teardown();
//      try {
//        FileUtils.deleteDirectory(new File(this.dataDirectory));
//        FileUtils.deleteDirectory(new File(this.recoveryDirectory));
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//    }
//  }
//
//  @Rule
//  public TestRecursiveFileSplitter testRecursiveMeta = new TestRecursiveFileSplitter();
//
//  private static String DIR_1 = "dir1.1";
//  private static String DIR_2_1 = "dir2.1";
//  private static String DIR_2_2 = "d2.2";
//  private static String DIR_2_3 = "dir23";
//  private static String DIR_2 = DIR_2_1 + "/" + DIR_2_2 + "/" + DIR_2_3;
//
//  @Test
//  public void testRecursiveScanSanity()
//  {
//    int numFiles = 2;
//    int numLines = 5;
//    int lineSize = 5;
//    int numDirs = 1;
//    mkdir(testRecursiveMeta.dataDirectory + DIR_1);
//    createFiles(testRecursiveMeta.dataDirectory + DIR_1, 2, 5);
//
//    int blockSize = 10;
//    testRecursiveMeta.fileSplitter.setBlockSize(new Long(blockSize));
//    testRecursiveMeta.fileSplitter.beginWindow(1);
//    testRecursiveMeta.fileSplitter.emitTuples();
//
//    int noOfBlocks = numFiles * (int) Math.ceil(((1.0 * numLines * lineSize) / blockSize));
//    Assert.assertEquals("Blocks ", noOfBlocks, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
//    Assert.assertEquals("Files ", numDirs + numFiles, testRecursiveMeta.fileMetadataSink.collectedTuples.size());
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    ArrayList<IngestionFileMetaData> fileList = (ArrayList) testRecursiveMeta.fileMetadataSink.collectedTuples;
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "dir1.1/file0.txt"));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "dir1.1/file1.txt"));
//    Assert.assertEquals("Missing entry:", false, hasFileEntry(fileList, "dir1.1/file2.txt"));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "dir1.1")); // No block entry for directory
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    ArrayList<FileBlockMetadata> blockList = (ArrayList) testRecursiveMeta.blockMetadataSink.collectedTuples;
//    Assert.assertEquals("Missing entry:", false, hasBlockEntry(blockList, "dir1.1"));
//    Assert.assertEquals("Missing entry:", true, hasBlockEntry(blockList, "dir1.1/file0.txt"));
//  }
//
//  @Test
//  public void testRecursiveScanMultiDepth()
//  {
//    int numFiles = 1;
//    int numLines = 6;
//    int lineSize = 5;
//    int numDirs = 3;
//    mkdir(testRecursiveMeta.dataDirectory + DIR_2);
//    createFiles(testRecursiveMeta.dataDirectory + DIR_2, 1, 6); // 1 files of 6 lines ( each line is 5 bytes) ==> size =
//                                                                // 30
//
//    int blockSize = 10;
//    testRecursiveMeta.fileSplitter.setBlockSize(new Long(blockSize));
//    testRecursiveMeta.fileSplitter.beginWindow(1);
//    testRecursiveMeta.fileSplitter.emitTuples();
//
//    int noOfBlocks = numFiles * (int) Math.ceil(((1.0 * numLines * lineSize) / blockSize));
//    Assert.assertEquals("Blocks", noOfBlocks, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
//    Assert.assertEquals("Blocks", numDirs + numFiles, testRecursiveMeta.fileMetadataSink.collectedTuples.size());
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    ArrayList<IngestionFileMetaData> fileList = (ArrayList) testRecursiveMeta.fileMetadataSink.collectedTuples;
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2_1));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2_1 + "/" + DIR_2_2));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2_1 + "/" + DIR_2_2 + "/" + DIR_2_3));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2)); // same as line above.
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2 + "/file0.txt"));
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    ArrayList<FileBlockMetadata> blockList = (ArrayList) testRecursiveMeta.blockMetadataSink.collectedTuples;
//    Assert.assertEquals("Missing entry:", false, hasBlockEntry(blockList, DIR_2_1));
//    Assert.assertEquals("Missing entry:", true, hasBlockEntry(blockList, DIR_2 + "/file0.txt"));
//  }
//
//  @Test
//  public void testNonRecursiveScanListing() throws IOException
//  {
//    String dir = testRecursiveMeta.dataDirectory;
//    FileUtils.touch(new File(dir, "1.txt"));
//    FileUtils.touch(new File(dir, "2.txt"));
//    mkdir(testRecursiveMeta.dataDirectory + DIR_1);
//
//    ((RecursiveDirectoryScanner)testRecursiveMeta.fileSplitter.getScanner()).setRecursiveScan(false);
//
//    testRecursiveMeta.fileSplitter.beginWindow(1);
//    testRecursiveMeta.fileSplitter.emitTuples();
//    ArrayList<IngestionFileMetaData> fileList = (ArrayList) testRecursiveMeta.fileMetadataSink.collectedTuples;
//    //Assert.assertEquals("No of file not matching", 0, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
//    Assert.assertEquals("Missing entry:", false, hasFileEntry(fileList, DIR_1));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "1.txt"));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, "2.txt"));
//  }
//
//  @Test
//  public void testNonRecursiveScan()
//  {
//    int numFiles = 1;
//    int numLines = 6;
//    ((RecursiveDirectoryScanner)testRecursiveMeta.fileSplitter.getScanner()).setRecursiveScan(false);
//    mkdir(testRecursiveMeta.dataDirectory + DIR_2);
//    createFiles(testRecursiveMeta.dataDirectory + DIR_2, numFiles, numLines); // 1 files of 6 lines ( each line is 5 bytes) ==> size =
//
//    int blockSize = 10;
//    testRecursiveMeta.fileSplitter.setBlockSize(new Long(blockSize));
//    testRecursiveMeta.fileSplitter.beginWindow(1);
//    testRecursiveMeta.fileSplitter.emitTuples();
//    Assert.assertEquals("No of file not matching", 0, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
//
//  }
//
//
//  @Test
//  public void testRecursiveScanZeroFileSize()
//  {
//    mkdir(testRecursiveMeta.dataDirectory + DIR_1);
//    try {
//      FileUtils.touch(new File(testRecursiveMeta.dataDirectory + DIR_1 + "/zero.txt"));
//    } catch (IOException e) {
//      throw new RuntimeException("Unable to create zero size file.");
//    }
//    int blockSize = 10;
//    testRecursiveMeta.fileSplitter.setBlockSize(new Long(blockSize));
//    testRecursiveMeta.fileSplitter.beginWindow(1);
//    testRecursiveMeta.fileSplitter.emitTuples();
//
//    Assert.assertEquals("Blocks", 0, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
//    Assert.assertEquals("Blocks", 2, testRecursiveMeta.fileMetadataSink.collectedTuples.size());
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    ArrayList<IngestionFileMetaData> list = (ArrayList) testRecursiveMeta.fileMetadataSink.collectedTuples;
//    Assert.assertEquals("Directory entry mismatch", 0, list.get(0).getFileLength());
//    Assert.assertEquals("Directory entry mismatch", true, list.get(0).isDirectory());
//    Assert.assertEquals("Directory name mismatch", DIR_1, list.get(0).getRelativePath());
//    Assert.assertEquals("Zero file entry mismatch", 0, list.get(1).getFileLength());
//    Assert.assertEquals("Zero file entry mismatch", false, list.get(1).isDirectory());
//    Assert.assertEquals("Directory name mismatch", DIR_1 + "/zero.txt", list.get(1).getRelativePath());
//
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(list, "dir1.1/zero.txt"));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(list, "dir1.1"));
//  }
//
//  @Test
//  public void testRecursiveScanMultiInput()
//  {
//    int numFiles = 3;
//    int numLines = 6;
//    int lineSize = 5;
//    int numDirs = 4;
//    mkdir(testRecursiveMeta.dataDirectory + DIR_1);
//    mkdir(testRecursiveMeta.dataDirectory + DIR_2);
//    createFiles(testRecursiveMeta.dataDirectory + DIR_1, 2, 6); // 2 files of size 30 bytes each.
//    createFiles(testRecursiveMeta.dataDirectory + DIR_2, 1, 6); // 1 files of 6 lines ( each line is 5 bytes)==>size =30
//
//    String inputDir = testRecursiveMeta.dataDirectory + "/" + DIR_1 + "," + testRecursiveMeta.dataDirectory + "/" + DIR_2_1;
//    System.out.println("Setting input directory: " + inputDir);
//    testRecursiveMeta.fileSplitter.setDirectory(inputDir);// pass two directories.
//
//    int blockSize = 10;
//    testRecursiveMeta.fileSplitter.setBlockSize(new Long(blockSize));
//    testRecursiveMeta.fileSplitter.beginWindow(1);
//    testRecursiveMeta.fileSplitter.emitTuples();
//
//    int noOfBlocks = numFiles * (int) Math.ceil(((1.0 * numLines * lineSize) / blockSize));
//    Assert.assertEquals("Blocks", noOfBlocks, testRecursiveMeta.blockMetadataSink.collectedTuples.size());
//    Assert.assertEquals("Blocks", numDirs + numFiles, testRecursiveMeta.fileMetadataSink.collectedTuples.size());
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    ArrayList<IngestionFileMetaData> fileList = (ArrayList) testRecursiveMeta.fileMetadataSink.collectedTuples;
//
//    // For DIR_1
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_1));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_1 + "/file0.txt"));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_1 + "/file1.txt"));
//
//    // For DIR_2
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2_1));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2_1 + "/" + DIR_2_2));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2_1 + "/" + DIR_2_2 + "/" + DIR_2_3));
//    Assert.assertEquals("Missing entry:", true, hasFileEntry(fileList, DIR_2 + "/file0.txt"));
//
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    ArrayList<FileBlockMetadata> blockList = (ArrayList) testRecursiveMeta.blockMetadataSink.collectedTuples;
//    Assert.assertEquals("Missing entry:", false, hasBlockEntry(blockList, DIR_1));
//    Assert.assertEquals("Missing entry:", true, hasBlockEntry(blockList, DIR_1 + "/file0.txt"));
//    Assert.assertEquals("Missing entry:", true, hasBlockEntry(blockList, DIR_1 + "/file1.txt"));
//    Assert.assertEquals("Missing entry:", false, hasBlockEntry(blockList, DIR_2_1));
//    Assert.assertEquals("Missing entry:", true, hasBlockEntry(blockList, DIR_2 + "/file0.txt"));
//  }
//
//  private boolean hasFileEntry(ArrayList<IngestionFileMetaData> list, String fileName)
//  {
//    for (IngestionFileMetaData fm : list) {
//      if (fm.getRelativePath().compareTo(fileName) == 0) {
//        return true;
//      }
//    }
//    return false;
//  }
//
//  private boolean hasBlockEntry(ArrayList<FileBlockMetadata> list, String fileName)
//  {
//    for (FileBlockMetadata fm : list) {
//      System.out.println(fm.getFilePath());
//      if (fm.getFilePath().endsWith(fileName)) {
//        return true;
//      }
//    }
//    return false;
//  }
//
//  public void mkdir(String dir)
//  {
//    new File(dir).mkdirs();
//  }
//
//  private void createFiles(String basePath, int numFiles, int numLines)
//  {
//    try {
//
//      HashSet<String> allLines = Sets.newHashSet();
//      for (int file = 0; file < numFiles; file++) {
//        HashSet<String> lines = Sets.newHashSet();
//        for (int line = 0; line < numLines; line++) {
//          lines.add("f" + file + "l" + line);
//        }
//        allLines.addAll(lines);
//        File created = new File(basePath, "/file" + file + ".txt");
//        testRecursiveMeta.filePaths.add("file:" + created.getAbsolutePath());
//        FileUtils.write(created, StringUtils.join(lines, '\n') + '\n');
//      }
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }

}
