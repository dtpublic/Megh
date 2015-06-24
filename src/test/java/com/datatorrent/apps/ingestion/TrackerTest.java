package com.datatorrent.apps.ingestion;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.malhar.lib.io.fs.FileSplitter.FileMetadata;

public class TrackerTest
{
  public static class TestMeta extends TestWatcher
  {
    private OperatorContext context;
    public Tracker tracker;

    public String baseDir = "";
    private String filePath = "someDummyPath";
    @Mock
    private FileMetadata fileMetadataMock;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();

      this.baseDir = "target" + Path.SEPARATOR + className + Path.SEPARATOR + description.getMethodName();
      this.filePath = baseDir + Path.SEPARATOR + "someDummyFile.txt";

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.CHECKPOINT_WINDOW_COUNT, 2);
      attributes.put(DAGContext.APPLICATION_PATH, baseDir);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      
      MockitoAnnotations.initMocks(this);
      when(fileMetadataMock.getFilePath()).thenReturn(filePath);
      when(fileMetadataMock.getBlockIds()).thenReturn(new long[] { 1, 2, 3 });
      
      tracker = new Tracker();
      
      tracker.setup(context);
      
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      tracker.teardown();
      try {
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testOneTimeCopy() throws IOException
  {
    testMeta.tracker.setOneTimeCopy(true);
    testMeta.tracker.setTimeoutWindowCount(2);
    runWindow(1, testMeta.fileMetadataMock, null);
    runWindow(2, null, testMeta.fileMetadataMock);
    runWindow(3, null, null);
    runWindow(4, null, null);
    runWindow(5, null, null);
    Path f = new Path(testMeta.tracker.oneTimeCopySignal);
    Assert.assertTrue("Shutdown signal file does not exist", testMeta.tracker.appFS.exists(f));
  }

  @Test(expected = RuntimeException.class)
  public void testFileMergerTupleReachingBeforeSplitter()
  {
    testMeta.tracker.setOneTimeCopy(true);
    testMeta.tracker.setup(testMeta.context);
    runWindow(1, null, testMeta.fileMetadataMock);
    runWindow(2, testMeta.fileMetadataMock, null);
  }

  public void runWindow(long windowId, FileMetadata inputOnFileSplitter, FileMetadata inputOnFileMerger)
  {
    testMeta.tracker.beginWindow(windowId);
    if (inputOnFileSplitter != null) {
      testMeta.tracker.inputFileSplitter.process(inputOnFileSplitter);
    }
    if (inputOnFileMerger != null) {
      testMeta.tracker.inputFileMerger.process(inputOnFileMerger);
    }
    testMeta.tracker.endWindow();
  }
  
  @Test
  public void testBlockDeletion() throws IOException{
      //Create 5 block files
      for(int i=0; i< 5; i++){
        FileUtils.write(new File(testMeta.tracker.blocksDir , Integer.toString(i)), "Some dummy data");
      }

      //Send FileMetaData from filesplitter
      runWindow(1, testMeta.fileMetadataMock, null);
      //Send completion signal to tracker
      runWindow(2, null, testMeta.fileMetadataMock);
      
      //Blocks 1,2,3 should have been deleted
      for(long i : testMeta.fileMetadataMock.getBlockIds() ){
        Assert.assertFalse("Block files not deleted", 
            testMeta.tracker.appFS.exists(
            new Path(testMeta.tracker.blocksDir , Long.toString(i))));
      }
      
      //Blocks 0,4 should have been deleted
      for(long i : new long[]{0,4} ){
        Assert.assertTrue("Block files not found", 
            testMeta.tracker.appFS.exists(
            new Path(testMeta.tracker.blocksDir , Long.toString(i))));
      }
      
  }
}
