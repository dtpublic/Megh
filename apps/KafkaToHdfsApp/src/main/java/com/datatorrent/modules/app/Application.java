/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.module.KafkaInputModule;
import com.datatorrent.module.io.fs.HDFSOutputModule;


@ApplicationAnnotation(name = "Kafka2Hdfs")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInputModule input = dag.addModule("KafkaReader", new KafkaInputModule());
    HDFSOutputModule output = dag.addModule("HDFSWriter", new HDFSOutputModule());
    dag.addStream("Messages", input.output, output.input);
  }

}

