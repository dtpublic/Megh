package com.datatorrent.apps.ingestion.kafka;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.TestUtils.TestInfo;

public class FileOutputOperatorTest
{
  private FileOutputOperator underTest;
  private static int operatorId = 4;
  private OperatorContextTestHelper.TestIdOperatorContext testOperatorContext = new OperatorContextTestHelper.TestIdOperatorContext(operatorId);

  @Rule
  public TestInfo testMeta = new FSTestWatcher();

  public static class FSTestWatcher extends TestInfo
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      new File(getDir()).mkdir();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }
  }

  @Before
  public void setup()
  {
    underTest = new FileOutputOperator();
    underTest.setFilePath(testMeta.getDir());
    underTest.setup(testOperatorContext);
  }

  @Test
  public void testFileName()
  {
    String expectedFileName = FileOutputOperator.OUTPUT_FILENAME + "." + operatorId;
    Assert.assertTrue(expectedFileName.equals(underTest.getFileName("tuple")));
  }
}
