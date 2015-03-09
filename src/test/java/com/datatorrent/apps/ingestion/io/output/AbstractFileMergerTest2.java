package com.datatorrent.apps.ingestion.io.output;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class AbstractFileMergerTest2
{
  private static final String APP_PATH = "target/user/hadoop/datatorrent/apps";
  private static final String OUTPUT_PATH = "target/user/appuser/output";
  private static final String OUTPUT_FILE_NAME = "output.txt";
  private static final int OPERATOR_ID = 0;
  private static OperatorContextTestHelper.TestIdOperatorContext context;
  private AbstractFileMerger underTest;
  @Mock
  private IngestionFileMetaData fileMetaDataMock;
  private long [] blockIds = new long[]{};
  
  @Before
  public void setup()
  {
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAGContext.APPLICATION_PATH, APP_PATH);
    context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    underTest = new AbstractFileMerger();
    underTest.setOutputDir(OUTPUT_PATH);
    underTest.setup(context);

    MockitoAnnotations.initMocks(this);
    when(fileMetaDataMock.getFileName()).thenReturn(OUTPUT_FILE_NAME);
    when(fileMetaDataMock.getRelativePath()).thenReturn(OUTPUT_FILE_NAME);
  }

  @Test
  public void testBlocksPath()
  {
    Assert.assertEquals("Blocks path not initialized in application context", context.getValue(DAGContext.APPLICATION_PATH) + File.separator + BlockWriter.SUBDIR_BLOCKS, underTest.getBlocksDir());
  }

  @Test
  public void testOverwriteFlag() throws IOException
  {
    FileUtils.write(new File(OUTPUT_PATH, OUTPUT_FILE_NAME), "");
    when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(fileMetaDataMock.isDirectory()).thenReturn(false);
    when(fileMetaDataMock.getBlockIds()).thenReturn(blockIds);

    underTest.setOverwriteOutputFile(false);
    underTest.processCommittedData(fileMetaDataMock);
    Assert.assertTrue("File overwrite not skipped", underTest.getSkippedFilesList().size() == 0);
    File statsFile = new File(context.getValue(DAGContext.APPLICATION_PATH) + File.separator + AbstractFileMerger.STATS_DIR + File.separator + AbstractFileMerger.SKIPPED_FILE);
    Assert.assertTrue(statsFile.exists());
    String fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(OUTPUT_FILE_NAME));
    

    underTest.getSkippedFilesList().clear();

    underTest.setOverwriteOutputFile(true);
    underTest.processCommittedData(fileMetaDataMock);
    Assert.assertTrue("File overwrite skipped", underTest.getSkippedFilesList().size() == 0);
    fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(OUTPUT_FILE_NAME));
  }

  @Test
  public void testSkippedFilePersistance() throws IOException
  {
    FileUtils.write(new File(OUTPUT_PATH, OUTPUT_FILE_NAME), "");
    when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(fileMetaDataMock.isDirectory()).thenReturn(false);

    underTest.setOverwriteOutputFile(false);
    underTest.processCommittedData(fileMetaDataMock);

    File statsFile = new File(context.getValue(DAGContext.APPLICATION_PATH) + File.separator + AbstractFileMerger.STATS_DIR + File.separator + AbstractFileMerger.SKIPPED_FILE);
    Assert.assertTrue(statsFile.exists());
    String fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(OUTPUT_FILE_NAME));
  }

  @Test
  public void testSkippedFileRecovery() throws IOException
  {
    underTest.skippedListFileLength = 12;
    String skippedFileNames = "skippedFile1\nskippedFile2";
    File statsFile = new File(context.getValue(DAGContext.APPLICATION_PATH) + File.separator + AbstractFileMerger.STATS_DIR + File.separator + AbstractFileMerger.SKIPPED_FILE);
    FileUtils.write(statsFile, skippedFileNames);
    underTest.setup(context);
    String fileData = FileUtils.readFileToString(statsFile);
    Assert.assertTrue(fileData.contains(skippedFileNames.substring(0, (int) underTest.skippedListFileLength)));
  }

  @Test
  public void testOverwriteFlagForDirectory() throws IOException, InterruptedException
  {
    FileUtils.forceMkdir(new File(OUTPUT_FILE_NAME));
    when(fileMetaDataMock.isDirectory()).thenReturn(true);
    underTest.setOverwriteOutputFile(true);
    underTest.beginWindow(1L);
    underTest.input.process(fileMetaDataMock);
    underTest.endWindow();
    underTest.checkpointed(1);
    underTest.committed(1);
    Thread.sleep(1000L);
    
    Assert.assertTrue("Directory overwrite failed", underTest.getSkippedFilesList().size() == 0);
    File statsFile = new File(fileMetaDataMock.getRelativePath());
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(new File(OUTPUT_PATH));
    FileUtils.deleteDirectory(new File(APP_PATH));
    
  }

}
