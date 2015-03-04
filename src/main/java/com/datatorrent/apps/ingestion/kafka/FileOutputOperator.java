package com.datatorrent.apps.ingestion.kafka;

import com.datatorrent.lib.io.fs.AbstractWindowFileOutputOperator;

public class FileOutputOperator extends AbstractWindowFileOutputOperator<String>
{

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    return tuple.getBytes();
  }

}
