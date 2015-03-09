package com.datatorrent.apps.ingestion.io.output;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter.IngestionFileMetaData;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class AbstractFileMergerTest3
{
  private static OperatorContextTestHelper.TestIdOperatorContext context;
  private static long[] blockIds = new long[] { 1, 2, 3 };

  private static final String FILE_DATA = "0123456789";
  private static final String BLOCK1_DATA = "0123";
  private static final String BLOCK2_DATA = "4567";
  private static final String BLOCK3_DATA = "89";

  public static class TestAbstractFileMerger extends TestWatcher
  {
    public String recoveryDir = "";
    public String APP_PATH = "";
    public String blocksDir = "";
    public String outputDir = "";
    public String outputFileName = "";

    public AbstractFileMerger fileMerger;
    @Mock
    public IngestionFileSplitter.IngestionFileMetaData fileMetaDataMock;

    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();

      this.APP_PATH = "target"+ Path.SEPARATOR + className + Path.SEPARATOR +  description.getMethodName();
      this.blocksDir = APP_PATH + Path.SEPARATOR + "blocks";
      this.recoveryDir = APP_PATH + Path.SEPARATOR + "recovery";
      this.outputDir = APP_PATH + Path.SEPARATOR + "output";
      outputFileName = "output.txt";

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getMethodName());
      attributes.put(DAGContext.APPLICATION_PATH, APP_PATH);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(APP_PATH).getAbsolutePath()), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.fileMerger = new AbstractFileMerger();
      this.fileMerger.setOutputDir(outputDir);
      this.fileMerger.setup(context);
      
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
      this.fileMerger.teardown();
      try {
        FileUtils.deleteDirectory(new File(this.APP_PATH));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestAbstractFileMerger underTest = new TestAbstractFileMerger();

  @Test
  public void testMergeFile() throws IOException
  {
    String blocksDir = underTest.APP_PATH + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS + Path.SEPARATOR;
    FileUtils.write(new File(blocksDir + blockIds[0]), BLOCK1_DATA);
    FileUtils.write(new File(blocksDir + blockIds[1]), BLOCK2_DATA);
    FileUtils.write(new File(blocksDir + blockIds[2]), BLOCK3_DATA);
    underTest.fileMerger.mergeFile(underTest.fileMetaDataMock);
    Assert.assertEquals("File size differes", FILE_DATA.length(), FileUtils.sizeOf(new File(underTest.outputDir, underTest.outputFileName)));
  }

}
