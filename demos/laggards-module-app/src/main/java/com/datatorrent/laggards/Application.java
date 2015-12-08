/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * ALL Rights Reserved.
 */

package com.datatorrent.demos.laggards;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;

import com.datatorrent.demos.laggards.parser.JsonParser;
import com.datatorrent.demos.laggards.parser.TimeInfo;

import com.datatorrent.modules.laggards.LaggardsModule;

@ApplicationAnnotation(name = "Laggards")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    /* Kafka Input Operator */
    KafkaSinglePortStringInputOperator in = dag.addOperator("Kafka", new KafkaSinglePortStringInputOperator());
    in.setConsumer(new SimpleKafkaConsumer());

    /* Parser Operator */
    JsonParser<TimeInfo> parser = dag.addOperator("Parser", new JsonParser<TimeInfo>());
    parser.setOutputClass(TimeInfo.class);
    dag.addStream("parser", in.outputPort, parser.in);

    /* Laggards Operator */
    LaggardsModule laggards = dag.addModule("Laggards", new LaggardsModule());
    dag.addStream("laggards", parser.out, laggards.input);
  }
}
