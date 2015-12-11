package com.datatorrent.modules.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.module.KafkaInputModule;

@ApplicationAnnotation(name = "KafkaBenchmarkApp")
public class KafkaBenchmarkApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInputModule input = dag.addModule("KafkaReader", new KafkaInputModule());
    DevNull<byte[]> output = dag.addOperator("devNull", DevNull.class);
    dag.addStream("Messages", input.output, output.data).setLocality(DAG.Locality.THREAD_LOCAL);
  }
}
