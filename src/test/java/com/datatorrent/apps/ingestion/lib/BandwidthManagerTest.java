package com.datatorrent.apps.ingestion.lib;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class BandwidthManagerTest
{
  private static class TestMeta extends TestWatcher
  {
    private String applicationPath;
    private BandwidthManager underTest;
    private Context.OperatorContext context;
    private long bandwidthLimit = 10L;
    private ScheduledExecutorTestService mockschedular;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      mockschedular = new ScheduledExecutorTestService();
      underTest = new BandwidthManager(mockschedular);
      underTest.setBandwidth(bandwidthLimit);

      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_PATH, applicationPath);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      underTest.setup(context);
    }

    @Override
    protected void finished(Description description)
    {
      underTest.teardown();
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testBandwidthForLargeBlocks() throws InterruptedException
  {
    String data = "Tuple: test data to be emitted.";
    long startTime = System.currentTimeMillis();
    while (!testMeta.underTest.canConsumeBandwidth(data.length())) {
      Thread.sleep(1000);
      testMeta.mockschedular.execute(null); // accumulate bandwidth
    }
    testMeta.underTest.consumeBandwidth(data.length());
    long endTime = System.currentTimeMillis();
    Assert.assertTrue((endTime - startTime) > ((data.length() / testMeta.bandwidthLimit) * 1000));
  }

  @Test
  public void testBandwidthForSmallBlocks()
  {
    String data = "Tuple";
    testMeta.mockschedular.execute(null); // accumulating initial bandwidth
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(data.length()));
    testMeta.underTest.consumeBandwidth(data.length());
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(data.length()));
    testMeta.underTest.consumeBandwidth(data.length());
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth(data.length()));
  }

  @Test
  public void testBandwidthForMultipleBlocks()
  {
    int[] tupleSizes = { 5, 2, 5, 4, 10, 4, 25, 2 };
    testMeta.mockschedular.execute(null); // accumulating initial bandwidth
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[0]));
    testMeta.underTest.consumeBandwidth(tupleSizes[0]);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[1]));
    testMeta.underTest.consumeBandwidth(tupleSizes[1]);

    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth(tupleSizes[2]));
    testMeta.mockschedular.execute(null);
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[2]));
    testMeta.underTest.consumeBandwidth(tupleSizes[2]);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[3]));
    testMeta.underTest.consumeBandwidth(tupleSizes[3]);

    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth(tupleSizes[4]));
    testMeta.mockschedular.execute(null);
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[4]));
    testMeta.underTest.consumeBandwidth(tupleSizes[4]);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[5]));
    testMeta.underTest.consumeBandwidth(tupleSizes[5]);

    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth(tupleSizes[6]));
    testMeta.mockschedular.execute(null);
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth(tupleSizes[6]));
    testMeta.mockschedular.execute(null);
    Assert.assertFalse(testMeta.underTest.canConsumeBandwidth(tupleSizes[6]));
    testMeta.mockschedular.execute(null);
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[6]));
    testMeta.underTest.consumeBandwidth(tupleSizes[6]);

    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(tupleSizes[7]));
  }

  @Test
  public void testUnsetBandwidth()
  {
    testMeta.underTest.setBandwidth(0);
    Assert.assertTrue(testMeta.underTest.canConsumeBandwidth(128 * 1024 * 1024 * 1024));
  }
}
