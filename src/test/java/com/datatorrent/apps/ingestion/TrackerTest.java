package com.datatorrent.apps.ingestion;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.FileSplitter.FileMetadata;

public class TrackerTest
{
  public static class TestTracker extends TestWatcher
  {
    private OperatorContext context;
    public Tracker tracker;

    public String baseDir = "";
    private String filePath = "someDummyPath";
    private FileMetadata fileMetadata;
    FileSystem fs;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();

      this.baseDir = "target" + Path.SEPARATOR + className + Path.SEPARATOR + description.getMethodName() + Path.SEPARATOR;
      this.filePath = baseDir + Path.SEPARATOR + "someDummyFile.txt";

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
      attributes.put(DAGContext.APPLICATION_PATH, baseDir);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

      tracker = new Tracker();
      fileMetadata = new FileMetadata(filePath);
      try {
        fs = FileSystem.newInstance(new Configuration());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      tracker.teardown();
      try {
        fs.close();
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestTracker testTracker = new TestTracker();

  @Test
  public void testSanity() throws IOException
  {
    testTracker.tracker.setOneTimeCopy(true);
    testTracker.tracker.setTimeoutWindowCount(2);
    testTracker.tracker.setup(testTracker.context);
    runWindow(1, testTracker.fileMetadata, null);
    runWindow(2, null, testTracker.fileMetadata);
    runWindow(3, null, null);
    runWindow(4, null, null);
    Path f = new Path(testTracker.context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + Application.ONE_TIME_COPY_DONE_FILE);
    Assert.assertTrue("Shutdown signal file does not exist", testTracker.fs.exists(f));
  }

  @Test(expected = RuntimeException.class)
  public void testFileMergerTupleReachingBeforeSplitter()
  {
    testTracker.tracker.setOneTimeCopy(true);
    testTracker.tracker.setup(testTracker.context);
    runWindow(1, null, testTracker.fileMetadata);
    runWindow(2, testTracker.fileMetadata, null);
  }

  public void runWindow(long windowId, FileMetadata inputOnFileSplitter, FileMetadata inputOnFileMerger)
  {
    testTracker.tracker.beginWindow(windowId);
    if (inputOnFileSplitter != null) {
      testTracker.tracker.inputFileSplitter.process(inputOnFileSplitter);
    }
    if (inputOnFileMerger != null) {
      testTracker.tracker.inputFileMerger.process(inputOnFileMerger);
    }
    testTracker.tracker.endWindow();
  }
}
