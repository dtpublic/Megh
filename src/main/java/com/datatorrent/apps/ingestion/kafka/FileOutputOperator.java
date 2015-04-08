package com.datatorrent.apps.ingestion.kafka;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class FileOutputOperator extends AbstractFileOutputOperator<String>
{

  static final String OUTPUT_FILENAME = "kafkaData";

  @Override
  protected String getFileName(String tuple)
  {
    int operatorId = context.getId();
    return OUTPUT_FILENAME + "." + operatorId;
  }

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return tuple.getBytes();
  }

}
