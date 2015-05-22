package com.datatorrent.apps.ingestion.io.input;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.malhar.lib.io.IdempotentStorageManager;
import com.datatorrent.malhar.lib.io.IdempotentStorageManager.FSIdempotentStorageManager;
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
    public Set<String> filePaths = Sets.newHashSet();

    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();
      this.appDirectory = "target" + Path.SEPARATOR + className;
      this.dataDirectory = appDirectory+ Path.SEPARATOR + "data";

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

      fileSplitter.getScanner().setFilePatternRegularExp(".*[.]txt");
      fileSplitter.getScanner().setFiles(dataDirectory);
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
    assertEquals("Recovery path not initialized in application context",
      IngestionFileSplitter.IDEMPOTENCY_RECOVERY,
      ((FSIdempotentStorageManager) testMeta.fileSplitter.getIdempotentStorageManager()).getRecoveryPath());
    testMeta.fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
  }

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
//      try {
//        FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
//        HashSet<String> allLines = Sets.newHashSet();
//        for (int file = 0; file < 2; file++) {
//          HashSet<String> lines = Sets.newHashSet();
//          for (int line = 0; line < 5; line++) {
//            lines.add("f" + file + "l" + line);
//          }
//          allLines.addAll(lines);
//          File created = new File(this.dataDirectory, "file" + file + ".txt");
//          filePaths.add("file:" + created.getAbsolutePath());
//          FileUtils.write(created, StringUtils.join(lines, '\n'));
//        }
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//
//      this.fileSplitter = new IngestionFileSplitter();
//
//      IngestionFileSplitter.RecursiveDirectoryScanner scanner = new IngestionFileSplitter.RecursiveDirectoryScanner();
//      scanner.setFilePatternRegexp(".*[.]txt");
//      fileSplitter.setScanner(scanner);
//      fileSplitter.setDirectory(dataDirectory);
//      fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
//      fileSplitter.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, new Attribute.AttributeMap.DefaultAttributeMap()));
//
//      fileMetadataSink = new CollectorTestSink<Object>();
//      fileSplitter.filesMetadataOutput.setSink(fileMetadataSink);
//
//      blockMetadataSink = new CollectorTestSink<Object>();
//      fileSplitter.blocksMetadataOutput.setSink(blockMetadataSink);
//    }
//
//    @Override
//    protected void finished(Description description)
//    {
//       this.filePaths.clear();
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
//
//  @Test
//  public void testDirectoryScannerFiltering() throws Exception
//  {
//    String[] fileNames;
//    String positivePattern;
//    String negativePattern;
//    String[] results;
//
//    fileNames = new String[] { "file1.data", "file2.dat", "file3.data" };
//    positivePattern = ".*\\.dat";
//    negativePattern = "";
//    results = new String[] { "file2.dat" };
//    testDirectoryScanner(fileNames, positivePattern, negativePattern, results);
//
//    fileNames = new String[] { "file1.data", "file2.dat", "file3.data" };
//    positivePattern = ".*\\.dat";
//    negativePattern = ".*\\.dat";
//    results = new String[] { };
//    testDirectoryScanner(fileNames, positivePattern, negativePattern, results);
//
//    fileNames = new String[] { "file1.data", "file2.dat", "file3.data" };
//    positivePattern = ".*\\.data";
//    negativePattern = ".*\\.dat";
//    results = new String[] { "file1.data","file3.data" };
//    testDirectoryScanner(fileNames, positivePattern, negativePattern, results);
//
//    fileNames = new String[] { "file1.txt", "file2.txt", "file3.txt" };
//    positivePattern = ".*\\.txt";
//    negativePattern = ".*3\\.txt";
//    results = new String[] { "file1.txt","file2.txt" };
//    testDirectoryScanner(fileNames, positivePattern, negativePattern, results);
//
//    fileNames = new String[] { "file1.txt", "file2.txt_COPYING", "file3.txt" };
//    positivePattern = ".*\\.txt";
//    negativePattern = ".*_COPYING";
//    results = new String[] { "file1.txt","file3.txt" };
//    testDirectoryScanner(fileNames, positivePattern, negativePattern, results);
//  }
//
//
//  private void testDirectoryScanner(String[] fileNames, String positivePattern, String negativePattern, String[] results) throws Exception
//  {
//    String dir = testMeta.dataDirectory;
//    FileContext.getLocalFSFileContext().delete(new Path(new File(dir).getAbsolutePath()), true);
//    for (String fileName : fileNames) {
//      FileUtils.touch(new File(dir, fileName));
//    }
//
//    IngestionFileSplitter oper = testMeta.fileSplitter;
//    oper.setDirectory(dir);
//
//    RecursiveDirectoryScanner scanner = new RecursiveDirectoryScanner();
//    scanner.setFilePatternRegexp(positivePattern);
//    scanner.setIgnoreFilePatternRegexp(negativePattern);
//    oper.setScanner(scanner);
//
//    LinkedHashSet<Path> paths = scanner.scan(FileSystem.newInstance(new Path(dir).toUri(), new Configuration()), oper.filePathArray[0], new HashSet<String>());
//    List<String> passedNames = Lists.newArrayList();
//    for(Path path: paths){
//      passedNames.add(path.getName());
//    }
//    Collections.sort(passedNames);
//
//    Assert.assertArrayEquals("Directory scanner output not matching", results, passedNames.toArray());
//  }
}
