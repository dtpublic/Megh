package com.datatorrent.apps.ingestion.kafka;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class FileOutputOperator extends AbstractFileOutputOperator<String>
{

  private static final String OUTPUT_FILENAME = "kafkaData.txt";

  @Override
  protected String getFileName(String tuple)
  {
    return OUTPUT_FILENAME;
  }

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return tuple.getBytes();
  }

}
