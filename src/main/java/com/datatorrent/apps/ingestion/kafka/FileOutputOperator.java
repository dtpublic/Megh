package com.datatorrent.apps.ingestion.kafka;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class FileOutputOperator extends AbstractFileOutputOperator<String>
{

  @Override
  protected String getFileName(String tuple)
  {
    return "testRun.txt";
  }

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return tuple.getBytes();
  }

}
