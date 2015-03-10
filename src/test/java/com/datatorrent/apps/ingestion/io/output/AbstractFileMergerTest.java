package com.datatorrent.apps.ingestion.io.output;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
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
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class AbstractFileMergerTest
{
  private static OperatorContext context;
  private static long[] blockIds = new long[] { 1, 2, 3 };

  private static final String FILE_DATA = "0123456789";
  private static final String BLOCK1_DATA = "0123";
  private static final String BLOCK2_DATA = "4567";
  private static final String BLOCK3_DATA = "89";

  public static class TestAbstractFileMerger extends TestWatcher
  {
    public String recoveryDir = "";
    public String baseDir = "";
    public String blocksDir = "";
    public String outputDir = "";
    public String statsDir = "";
    public String outputFileName = "";

    public AbstractFileMerger underTest;
    @Mock
    public IngestionFileSplitter.IngestionFileMetaData fileMetaDataMock;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();

      this.baseDir = "target" + Path.SEPARATOR + className + Path.SEPARATOR + description.getMethodName();
      this.blocksDir = baseDir + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;
      this.recoveryDir = baseDir + Path.SEPARATOR + "recovery";
      this.outputDir = baseDir + Path.SEPARATOR + "output";
      this.statsDir = baseDir + Path.SEPARATOR + AbstractFileMerger.STATS_DIR;
      outputFileName = "output.txt";

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getMethodName());
      attributes.put(DAGContext.APPLICATION_PATH, baseDir);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(baseDir).getAbsolutePath()), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.underTest = new AbstractFileMerger();
      this.underTest.setOutputDir(outputDir);
      this.underTest.setup(context);

      MockitoAnnotations.initMocks(this);
      when(fileMetaDataMock.getFileName()).thenReturn(outputFileName);
      when(fileMetaDataMock.getRelativePath()).thenReturn(outputFileName);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(3);
      when(fileMetaDataMock.getBlockIds()).thenReturn(new long[] { 1, 2, 3 });
      when(fileMetaDataMock.isDirectory()).thenReturn(false);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(blockIds.length);
    }

    @Override
    protected void finished(Description description)
    {
      this.underTest.teardown();
      try {
        FileUtils.deleteDirectory(new File(this.baseDir));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
  }
  @AfterClass
  public static void cleanup()
  {
    System.out.println("**********************************");
    try {
      FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + AbstractFileMergerTest.class.getName()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Rule
  public TestAbstractFileMerger testFM = new TestAbstractFileMerger();

  @Test
  public void testMergeFile() throws IOException
  {
    String blocksDir = testFM.baseDir + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR;
    FileUtils.write(new File(blocksDir + blockIds[0]), BLOCK1_DATA);
    FileUtils.write(new File(blocksDir + blockIds[1]), BLOCK2_DATA);
    FileUtils.write(new File(blocksDir + blockIds[2]), BLOCK3_DATA);
    testFM.underTest.mergeFile(testFM.fileMetaDataMock);
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(testFM.outputDir, testFM.outputFileName)));
  }

  @Test(expected = RuntimeException.class)
  public void testMergeFileMissingBlock() throws IOException
  {
    String blocksDir = testFM.baseDir + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR;
    FileUtils.write(new File(blocksDir + blockIds[0]), BLOCK1_DATA);
    FileUtils.write(new File(blocksDir + blockIds[1]), BLOCK2_DATA);
    // FileUtils.write(new File(blocksDir + blockIds[2]), BLOCK3_DATA); MISSING BLOCK
    testFM.underTest.mergeFile(testFM.fileMetaDataMock);
  }

  @Test
  public void testBlocksPath()
  {
    Assert.assertEquals("Blocks path not initialized in application context", context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS, testFM.blocksDir);
  }

  @Test
  public void testOverwriteFlag() throws IOException
  {
    FileUtils.write(new File(testFM.outputDir, testFM.outputFileName), "");
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(false);
    when(testFM.fileMetaDataMock.getBlockIds()).thenReturn(new long[] {});

    // testFM.underTest.setOverwriteOutputFile(false);
    // testFM.underTest.processCommittedData(testFM.fileMetaDataMock);
    // File statsFile = new File(testFM.statsDir + Path.SEPARATOR + AbstractFileMerger.SKIPPED_FILE);
    // Assert.assertTrue(statsFile.exists());
    // String fileData = FileUtils.readFileToString(statsFile);
    // Assert.assertTrue(fileData.contains(testFM.outputFileName));

    testFM.underTest.setOverwriteOutputFile(true);
    testFM.underTest.processCommittedData(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.statsDir + Path.SEPARATOR + AbstractFileMerger.SKIPPED_FILE);
    Assert.assertFalse(statsFile.exists());

  }

  @Test
  public void testSkippedFilePersistance() throws IOException
  {
    FileUtils.write(new File(testFM.outputDir, testFM.outputFileName), "");
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(false);

    testFM.underTest.setOverwriteOutputFile(false);
    testFM.underTest.processCommittedData(testFM.fileMetaDataMock);

    File statsFile = new File(testFM.statsDir + Path.SEPARATOR + AbstractFileMerger.SKIPPED_FILE);
    Assert.assertTrue(statsFile.exists());
    String fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(testFM.outputFileName));
  }

  @Test
  public void testSkippedFileRecovery() throws IOException
  {
    testFM.underTest.skippedListFileLength = 12;
    String skippedFileNames = "skippedFile1\nskippedFile2";
    File statsFile = new File(testFM.statsDir + Path.SEPARATOR + AbstractFileMerger.SKIPPED_FILE);
    FileUtils.write(statsFile, skippedFileNames);
    testFM.underTest.setup(context);
    String fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(skippedFileNames.substring(0, (int) testFM.underTest.skippedListFileLength)));
  }

  // Using a bit of reconciler during testing, so using committed call explicitly
  @Test
  public void testOverwriteFlagForDirectory() throws IOException, InterruptedException
  {
    FileUtils.forceMkdir(new File(testFM.outputDir));
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(true);
    testFM.underTest.setOverwriteOutputFile(true);
    testFM.underTest.beginWindow(1L);
    testFM.underTest.input.process(testFM.fileMetaDataMock);
    testFM.underTest.endWindow();
    testFM.underTest.checkpointed(1);
    testFM.underTest.committed(1);
    Thread.sleep(1000L);

    File statsFile = new File(testFM.fileMetaDataMock.getRelativePath());
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

}
