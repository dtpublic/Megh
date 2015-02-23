package com.datatorrent.apps.ingestion.io.output;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class AbstractFileMergerTest
{
  
  public static class TestAbstractFileMerger extends TestWatcher
  {
    public String recoveryDirectory = null;

    public String appDir="";
    public String blocksDir="";
    public String outputDir="";
    
    public AbstractFileMerger fileMerger;
    @Mock
    public IngestionFileSplitter.IngestionFileMetaData fileMetaData;
    
    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();
      
      this.appDir= "target/" + className + "/" + "apps/";
      this.blocksDir = appDir + "blocks/";
      this.recoveryDirectory = appDir + "recovery/";

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getMethodName());
      attributes.put(DAG.APPLICATION_PATH,appDir);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      
      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(appDir).getAbsolutePath()), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      this.fileMerger = new AbstractFileMerger() ;
      this.fileMerger.setFilePath(outputDir);

      
      MockitoAnnotations.initMocks(this);
      when(fileMetaData.getNumberOfBlocks()).thenReturn(3);
      when(fileMetaData.getBlockIds()).thenReturn(new long[]{1,2,3});
      when(fileMetaData.isDirectory()).thenReturn(false);
    }

    @Override
    protected void finished(Description description)
    {
      this.fileMerger.teardown();
      try {
        FileUtils.deleteDirectory(new File(this.appDir));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    protected void createBlocks(String blockStart, int numBlocks, int blockSize)
    {
      
    }
    protected void createBlock(String blockNum, int  blockSize) throws IOException
    {
      char [] chrArray = new char[blockSize];
//      for(int i=0;i<blockSize;i++){
//        
//      }
      FileUtils.write(new File(blockNum), chrArray.toString().subSequence(0, blockSize));
    }
  }
  
  @Rule
  public TestAbstractFileMerger underTest = new TestAbstractFileMerger();

  @Test
  public void testMergeFile()
  {
    underTest.fileMerger.processedFileInput.process(underTest.fileMetaData);
  }

  @Test
  public void testMoveFile()
  {
  }

}
