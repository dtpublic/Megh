package com.datatorrent.apps.ingestion.io.input;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.Application;
import com.datatorrent.apps.ingestion.TrackerEvent;
import com.datatorrent.apps.ingestion.TrackerEvent.TrackerEventType;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.Scanner;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.IdempotentStorageManager.FSIdempotentStorageManager;
import com.google.common.collect.Sets;

public class IngestionFileSplitterTest
{
  public static class TestBaseFileSplitter extends TestWatcher
  {
    public String dataDirectory = null;
    public String appDirectory = null;

    public IngestionFileSplitter fileSplitter;
    public CollectorTestSink<Object> fileMetadataSink;
    public CollectorTestSink<Object> blockMetadataSink;
    public CollectorTestSink<Object> trackerOutPortSink;
    public Set<String> filePaths = Sets.newHashSet();

    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();
      this.appDirectory = "target" + Path.SEPARATOR + className;
      this.dataDirectory = appDirectory + Path.SEPARATOR + "data";

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "IngestionFileSplitterTest");
      attributes.put(DAG.DAGContext.APPLICATION_PATH, appDirectory);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

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

      fileSplitter.setBlocksThreshold(10);
      fileSplitter.getScanner().setFiles("file://" + new File(dataDirectory).getAbsolutePath());
      fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
      fileSplitter.getScanner().setScanIntervalMillis(1000);

      fileMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.filesMetadataOutput.setSink(fileMetadataSink);

      blockMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.blocksMetadataOutput.setSink(blockMetadataSink);
      

      trackerOutPortSink = new CollectorTestSink<Object>();
      fileSplitter.trackerOutPort.setSink(trackerOutPortSink);
    }

    @Override
    protected void finished(Description description)
    {
      this.fileSplitter.teardown();
      try {
        FileUtils.deleteDirectory(new File(this.appDirectory));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestBaseFileSplitter testMeta = new TestBaseFileSplitter();

  @Test
  public void testRecoveryPath()
  {
    testMeta.fileSplitter.setIdempotentStorageManager(new FSIdempotentStorageManager());
    testMeta.fileSplitter.setup(testMeta.context);
    assertEquals("Recovery path not initialized in application context", IngestionFileSplitter.IDEMPOTENCY_RECOVERY, ((FSIdempotentStorageManager) testMeta.fileSplitter.getIdempotentStorageManager()).getRecoveryPath());
    testMeta.fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
  }

  @Test
  public void testCompressionExtension() throws Exception
  {
    String compressionExt = Application.GZIP_FILE_EXTENSION;
    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.setcompressionExtension(compressionExt);

    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Object fileMetadata = testMeta.fileMetadataSink.collectedTuples.get(0);
    IngestionFileSplitter.IngestionFileMetaData metadata = (IngestionFileSplitter.IngestionFileMetaData) fileMetadata;
    String relativePath = metadata.getRelativePath();
    String fileExt = relativePath.substring(relativePath.lastIndexOf(".") + 1, relativePath.length());
    Assert.assertEquals(compressionExt, fileExt);

    testMeta.fileMetadataSink.collectedTuples.clear();
  }

  @Test
  public void testRelativePath()
  {
    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Object fileMetadata = testMeta.fileMetadataSink.collectedTuples.get(0);
    IngestionFileSplitter.IngestionFileMetaData metadata = (IngestionFileSplitter.IngestionFileMetaData) fileMetadata;
    String relativePath = metadata.getRelativePath();
    File dataDir = new File(testMeta.dataDirectory);
    Assert.assertTrue(relativePath.startsWith(dataDir.getName()));
  }

  @Test
  public void testSpecialCharFileName() throws IOException
  {
    List<String> fileNames = Arrays.asList("filedated:12-3-2015.txt");
    createFilesInCleanDirectory(fileNames);

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals(0, testMeta.fileMetadataSink.collectedTuples.size());
  }
  
  @Test
  public void testNumberOfDiscoveredFiles() throws IOException, InterruptedException
  {
    Scanner ingestionScanner = ((Scanner) testMeta.fileSplitter.getScanner());

    List<String> fileNames = Arrays.asList("file1.txt", "file2.txt", "file3.txt");
    createFilesInCleanDirectory(fileNames);

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.getScanner().setScanIntervalMillis(9000);
    //Wait for some time for scanning.
    Thread.sleep(1000);
    
    long beforeEmit = ingestionScanner.getNoOfDiscoveredFilesInThisScan();
    testMeta.fileSplitter.emitTuples();
    long afterEmit = ingestionScanner.getNoOfDiscoveredFilesInThisScan();
    Assert.assertEquals(beforeEmit, afterEmit);
  }
  
  @Test
  public void testSkippedFiles() throws IOException, InterruptedException
  {
    ArrayList<String> expectedresults = new ArrayList<String>();

    List<String> fileNames = Arrays.asList("file1.txt", "file:2.txt", "file3.txt");
    expectedresults.addAll(Arrays.asList("file1.txt", "file3.txt"));
    createFilesInCleanDirectory(fileNames);

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    
    Assert.assertEquals(expectedresults.size(), testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals(TrackerEventType.SKIPPED_FILE, ((TrackerEvent)testMeta.trackerOutPortSink.collectedTuples.get(1)).getType());
  }

  @Test
  public void testDirectoryScannerFiltering() throws Exception
  {
    Scanner ingestionScanner = ((Scanner) testMeta.fileSplitter.getScanner());
    ingestionScanner.setOneTimeCopy(true);
    ArrayList<String> expectedresults = new ArrayList<String>();

    List<String> fileNames = Arrays.asList("file1.data", "file2.dat", "file3.data");
    expectedresults.addAll(Arrays.asList("file2.dat"));
    createFilesInCleanDirectory(fileNames);

    ingestionScanner.setFilePatternRegularExp(".*[.]dat");
    ingestionScanner.setIgnoreFilePatternRegularExp(".*[.]data");

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals(expectedresults.size(), testMeta.fileMetadataSink.collectedTuples.size());
    verifyFilteredResults(expectedresults, testMeta.fileMetadataSink.collectedTuples);
  }

  @Test
  public void testCancelingFilters() throws IOException
  {
    Scanner ingestionScanner = ((Scanner) testMeta.fileSplitter.getScanner());
    ingestionScanner.setOneTimeCopy(true);
    ArrayList<String> expectedresults = new ArrayList<String>();
    List<String> fileNames = Arrays.asList("file1.data", "file2.dat", "file3.data");
    createFilesInCleanDirectory(fileNames);

    ingestionScanner.setFilePatternRegularExp(".*[.]dat");
    ingestionScanner.setIgnoreFilePatternRegularExp(".*[.]dat");

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals(expectedresults.size(), testMeta.fileMetadataSink.collectedTuples.size());
    verifyFilteredResults(expectedresults, testMeta.fileMetadataSink.collectedTuples);
  }

  @Test
  public void testFilterNameStartsWith() throws IOException
  {
    Scanner ingestionScanner = ((Scanner) testMeta.fileSplitter.getScanner());
    ingestionScanner.setOneTimeCopy(true);
    ArrayList<String> expectedresults = new ArrayList<String>();
    expectedresults.addAll(Arrays.asList("file2.dat", "file3.data"));
    List<String> fileNames = Arrays.asList("myfile1.data", "file2.dat", "file3.data");
    createFilesInCleanDirectory(fileNames);

    ingestionScanner.setFilePatternRegularExp("file.*");

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals(expectedresults.size(), testMeta.fileMetadataSink.collectedTuples.size());
    verifyFilteredResults(expectedresults, testMeta.fileMetadataSink.collectedTuples);
  }

  @Test
  public void testChangeInFilter() throws IOException
  {
    Scanner ingestionScanner = ((Scanner) testMeta.fileSplitter.getScanner());
    ingestionScanner.setOneTimeCopy(true);
    ArrayList<String> expectedresults = new ArrayList<String>();
    List<String> fileNames = Arrays.asList("myfile1.data", "myfile2.dat", "file3.data");
    createFilesInCleanDirectory(fileNames);

    ingestionScanner.setFilePatternRegularExp("file.*");
    testMeta.fileMetadataSink.collectedTuples.clear();

    ingestionScanner.setFilePatternRegularExp("myfile.*");
    ingestionScanner.setIgnoreFilePatternRegularExp(".*[.]dat");
    expectedresults.addAll(Arrays.asList("myfile1.data"));
    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals(expectedresults.size(), testMeta.fileMetadataSink.collectedTuples.size());
    verifyFilteredResults(expectedresults, testMeta.fileMetadataSink.collectedTuples);
  }

  private void verifyFilteredResults(ArrayList<String> expectedresults, List<Object> collectedTuples)
  {
    for (Object tuple : collectedTuples) {
      IngestionFileSplitter.IngestionFileMetaData metadata = (IngestionFileSplitter.IngestionFileMetaData) tuple;
      int fileIndex = expectedresults.indexOf(metadata.getFileName());
      expectedresults.remove(fileIndex);
    }
    Assert.assertEquals("Failed filtering files.", expectedresults.size(), 0);
  }

  private void createFilesInCleanDirectory(List<String> fileNames) throws IOException
  {
    FileUtils.cleanDirectory(new File(testMeta.dataDirectory));
    for (String fileName : fileNames) {
      FileUtils.write(new File(testMeta.dataDirectory + File.separator + fileName), "testData");
      FileUtils.write(new File(testMeta.dataDirectory + File.separator + fileName), "testData");
    }
  }

  // @Test
  // public void testDefaultFilter() throws IOException
  // {
  // // ._COPYING_
  // Scanner ingestionScanner = ((Scanner) testMeta.fileSplitter.getScanner());
  // ingestionScanner.setOneTimeCopy(true);
  // ArrayList<String> expectedresults = new ArrayList<String>();
  // List<String> fileNames = Arrays.asList("file1.data._COPYING_", "file2._COPYING_", "file3.data");
  // expectedresults.addAll(Arrays.asList("file3.data"));
  // createFilesInCleanDirectory(fileNames);
  //
  // testMeta.fileSplitter.setup(testMeta.context);
  // testMeta.fileSplitter.beginWindow(1);
  // testMeta.fileSplitter.emitTuples();
  // testMeta.fileSplitter.endWindow();
  // Assert.assertEquals(expectedresults.size(), testMeta.fileMetadataSink.collectedTuples.size());
  // verifyFilteredResults(expectedresults, testMeta.fileMetadataSink.collectedTuples);
  // }

  // fileNames = new String[] { "file1.txt", "file2.txt", "file3.txt" };
  // positivePattern = ".*\\.txt";
  // negativePattern = ".*3\\.txt";
  // results = new String[] { "file1.txt","file2.txt" };
  // testDirectoryScanner(fileNames, positivePattern, negativePattern, results);
  //
  // fileNames = new String[] { "file1.txt", "file2.txt_COPYING", "file3.txt" };
  // positivePattern = ".*\\.txt";
  // negativePattern = ".*_COPYING";
  // results = new String[] { "file1.txt","file3.txt" };
  // testDirectoryScanner(fileNames, positivePattern, negativePattern, results);
  // private void testDirectoryScanner(String[] fileNames, String positivePattern, String negativePattern, String[]
  // results) throws Exception
  // {
  // String dir = testMeta.dataDirectory;
  // FileContext.getLocalFSFileContext().delete(new Path(new File(dir).getAbsolutePath()), true);
  // for (String fileName : fileNames) {
  // FileUtils.touch(new File(dir, fileName));
  // }
  //
  // IngestionFileSplitter oper = testMeta.fileSplitter;
  // // oper.setDirectory(dir);
  //
  // Scanner scanner = new Scanner();
  // // scanner.setFilePatternRegexp(positivePattern);
  // scanner.setIgnoreFilePatternRegularExp(negativePattern);
  // oper.setScanner(scanner);
  //
  // scanner.scan(new Path(dir), null);
  // List<String> passedNames = Lists.newArrayList();
  // FileSplitter.FileInfo fileInfo;
  // while(( fileInfo = scanner.pollFile()) != null)
  // passedNames.add(path.getName());
  // }
  // // Collections.sort(passedNames);
  //
  // Assert.assertArrayEquals("Directory scanner output not matching", results, passedNames.toArray());
  // }

  //
  // public static class TestRecursiveFileSplitter extends TestWatcher
  // {
  // public String dataDirectory = null;
  // public String recoveryDirectory = null;
  //
  // public IngestionFileSplitter fileSplitter;
  // public CollectorTestSink<Object> fileMetadataSink;
  // public CollectorTestSink<Object> blockMetadataSink;
  // public Set<String> filePaths = Sets.newHashSet();
  //
  // @Override
  // protected void starting(org.junit.runner.Description description)
  // {
  // String className = description.getClassName();
  // this.dataDirectory = "target/" + className + "/" + "data/";
  // this.recoveryDirectory = "target/" + className + "/" + "recovery/";
  //
  // try {
  // FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
  // HashSet<String> allLines = Sets.newHashSet();
  // for (int file = 0; file < 2; file++) {
  // HashSet<String> lines = Sets.newHashSet();
  // for (int line = 0; line < 5; line++) {
  // lines.add("f" + file + "l" + line);
  // }
  // allLines.addAll(lines);
  // File created = new File(this.dataDirectory, "file" + file + ".txt");
  // filePaths.add("file:" + created.getAbsolutePath());
  // FileUtils.write(created, StringUtils.join(lines, '\n'));
  // }
  // } catch (IOException e) {
  // throw new RuntimeException(e);
  // }
  //
  // this.fileSplitter = new IngestionFileSplitter();
  //
  // IngestionFileSplitter.RecursiveDirectoryScanner scanner = new IngestionFileSplitter.RecursiveDirectoryScanner();
  // scanner.setFilePatternRegexp(".*[.]txt");
  // fileSplitter.setScanner(scanner);
  // fileSplitter.setDirectory(dataDirectory);
  // fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
  // fileSplitter.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, new
  // Attribute.AttributeMap.DefaultAttributeMap()));
  //
  // fileMetadataSink = new CollectorTestSink<Object>();
  // fileSplitter.filesMetadataOutput.setSink(fileMetadataSink);
  //
  // blockMetadataSink = new CollectorTestSink<Object>();
  // fileSplitter.blocksMetadataOutput.setSink(blockMetadataSink);
  // }
  //
  // @Override
  // protected void finished(Description description)
  // {
  // this.filePaths.clear();
  // this.fileSplitter.teardown();
  // try {
  // FileUtils.deleteDirectory(new File(this.dataDirectory));
  // FileUtils.deleteDirectory(new File(this.recoveryDirectory));
  // } catch (IOException e) {
  // throw new RuntimeException(e);
  // }
  // }
  // }
  //
}
