package com.datatorrent.apps.ingestion.io.output;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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
  private static final int OPERATOR_ID = 0;
  private static OperatorContextTestHelper.TestIdOperatorContext context;
  private HdfsFileMerger underTest;
  @Mock
  private IngestionFileMetaData fileMetaDataMock;

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

}
