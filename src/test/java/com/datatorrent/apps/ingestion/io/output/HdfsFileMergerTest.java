package com.datatorrent.apps.ingestion.io.output;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class HdfsFileMergerTest
{
  private static final String APP_PATH = "user/hadoop/datatorrent/apps";
  private static final String OUTPUT_PATH = "user/appuser/output";
  private static final String OUTPUT_FILE_NAME = "output.txt";
  private static final String FILE_DATA ="0123456789";
  private static final String BLOCK1_DATA ="0123";
  private static final String BLOCK2_DATA ="4567";
  private static final String BLOCK3_DATA ="89";
  private static final int OPERATOR_ID = 0;
  private static OperatorContextTestHelper.TestIdOperatorContext context;
  private HdfsFileMerger underTest;
  @Mock
  private IngestionFileMetaData fileMetaDataMock;
  private long [] blockIds = new long[]{1,2,3};

  @Before
  public void setup()
  {
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_PATH, APP_PATH);
    context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    underTest = new HdfsFileMerger();
    underTest.setFilePath(OUTPUT_PATH);
    underTest.setup(context);

    MockitoAnnotations.initMocks(this);
    when(fileMetaDataMock.getFileName()).thenReturn(OUTPUT_FILE_NAME);
    when(fileMetaDataMock.getRelativePath()).thenReturn(OUTPUT_FILE_NAME);
    when(fileMetaDataMock.getBlockIds()).thenReturn(blockIds);
  }

  @Test
  public void testBlocksPath()
  {
    assertEquals("Blocks path not initialized in application context", context.getValue(DAG.APPLICATION_PATH) + File.separator + BlockWriter.SUBDIR_BLOCKS, underTest.blocksPath);
  }

  @Test
  public void testOverwriteFlag() throws IOException
  {
    FileUtils.write(new File(OUTPUT_PATH, OUTPUT_FILE_NAME), "");
    when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(fileMetaDataMock.isDirectory()).thenReturn(false);

    underTest.setOverwriteOutputFile(false);
    underTest.processedFileInput.process(fileMetaDataMock);
    Assert.assertTrue("File overwrite not skipped", underTest.getSkippedFilesList().size() == 1);

    underTest.getSkippedFilesList().clear();

    underTest.setOverwriteOutputFile(true);
    underTest.processedFileInput.process(fileMetaDataMock);
    Assert.assertTrue("File overwrite skipped", underTest.getSkippedFilesList().size() == 0);
  }

  @Test
  public void testOverwriteFlagForDirectory() throws IOException
  {
    FileUtils.forceMkdir(new File(OUTPUT_PATH, "dir1"));
    when(fileMetaDataMock.isDirectory()).thenReturn(true);
    underTest.setOverwriteOutputFile(true);
    underTest.processedFileInput.process(fileMetaDataMock);
    // for directory create if doesn't exist and no other processing should happen
  }

  @Test
  public void testAllBlocksPresent() throws IOException
  {
    IngestionFileMetaData iFileMetadata = new IngestionFileMetaData();
    iFileMetadata.setNumberOfBlocks(blockIds.length);
    iFileMetadata.setBlockIds(blockIds);
    iFileMetadata.setFileLength(10);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[0]), BLOCK1_DATA);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[1]), "4567");
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[2]), "89");
    Assert.assertTrue("All blocks present", underTest.allBlocksPresent(iFileMetadata));
  }

  /**
   * first block = file size, missing second block
   * 
   * @throws IOException
   */
  @Test
  public void testAllBlocksPresentMissingSecondBlock() throws IOException
  {
    IngestionFileMetaData iFileMetadata = new IngestionFileMetaData();
    iFileMetadata.setNumberOfBlocks(blockIds.length);
    iFileMetadata.setBlockIds(blockIds);
    iFileMetadata.setFileLength(10);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[0]), FILE_DATA);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + blockIds[2]), BLOCK3_DATA);
    Assert.assertFalse("One block missing", underTest.allBlocksPresent(iFileMetadata));
  }

  public void testRecoveryWithAllBlocks() throws IOException
  {
    IngestionFileMetaData iFileMetadata = new IngestionFileMetaData();
    iFileMetadata.setNumberOfBlocks(blockIds.length);
    iFileMetadata.setBlockIds(blockIds);
    iFileMetadata.setFileLength(10);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[0]), BLOCK1_DATA);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[1]), BLOCK2_DATA);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[2]), BLOCK3_DATA);
    Assert.assertTrue("All blocks present", underTest.recover(iFileMetadata));
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(OUTPUT_PATH, OUTPUT_FILE_NAME)));
  }

  /**
   * First block = file, no other block present
   * 
   * @throws IOException
   */
  public void testRecoveryWithMissingBlocks() throws IOException
  {
    IngestionFileMetaData iFileMetadata = new IngestionFileMetaData();
    iFileMetadata.setNumberOfBlocks(blockIds.length);
    iFileMetadata.setBlockIds(blockIds);
    iFileMetadata.setFileLength(10);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[0]), FILE_DATA);
    Assert.assertTrue("All blocks present", underTest.recover(iFileMetadata));
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(OUTPUT_PATH, OUTPUT_FILE_NAME)));
  }

  /**
   * First block = file, last two blocks are replayed during recovery.
   * 
   * @throws IOException
   */
  public void testRecoveryWithMissingFirstBlock() throws IOException
  {
    IngestionFileMetaData iFileMetadata = new IngestionFileMetaData();
    iFileMetadata.setNumberOfBlocks(blockIds.length);
    iFileMetadata.setBlockIds(blockIds);
    iFileMetadata.setFileLength(10);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[1]), BLOCK2_DATA);
    FileUtils.write(new File(APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR + blockIds[2]), BLOCK3_DATA); // missing
                                                                                                                                  // last
                                                                                                                                  // block
    Assert.assertFalse("All blocks present", underTest.recover(iFileMetadata));
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(OUTPUT_PATH, OUTPUT_FILE_NAME)));
  }

  public void testRecoveryAllMissingBlocksNFileAlreadyInOutput() throws IOException
  {
    IngestionFileMetaData iFileMetadata = new IngestionFileMetaData();
    iFileMetadata.setNumberOfBlocks(blockIds.length);
    iFileMetadata.setBlockIds(blockIds);
    iFileMetadata.setFileLength(10);
    FileUtils.write(new File(OUTPUT_PATH, OUTPUT_FILE_NAME), FILE_DATA); // File already at output location
    Assert.assertTrue("All blocks present", underTest.recover(iFileMetadata));
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(OUTPUT_PATH, OUTPUT_FILE_NAME)));
  }
  
  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(new File(OUTPUT_PATH));
    FileUtils.deleteDirectory(new File(APP_PATH));
    
  }

}
