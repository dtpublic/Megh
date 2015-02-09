package com.datatorrent.apps.ingestion.io.output;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;

import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class HdfsFileMergerTest
{
  private static final String APP_PATH = "/user/hadoop/datatorrent/apps";
  private static final String OUTPUT_PATH = "/user/appuser/output";
  private static final int OPERATOR_ID = 0;
  private static OperatorContextTestHelper.TestIdOperatorContext context;
  private HdfsFileMerger underTest;

  @Before
  public void setup()
  {
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_PATH, APP_PATH);
    context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    underTest = new HdfsFileMerger();
    underTest.setFilePath(OUTPUT_PATH);
    underTest.setup(context);
  }

  @Test
  public void testBlocksPath()
  {
    assertEquals("Blocks path not initialized in application context", context.getValue(DAG.APPLICATION_PATH) + File.separator + BlockWriter.SUBDIR_BLOCKS, underTest.blocksPath);
  }

}
