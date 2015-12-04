/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app.aggregation;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.schema.parser.CsvParser;
import com.datatorrent.modules.aggregation.AggregationModule;
import com.datatorrent.modules.app.aggregation.customdata.InputReader;
import com.datatorrent.modules.app.aggregation.customdata.Output;

@ApplicationAnnotation(name = "TestApp")
public class TestApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputReader reader = dag.addOperator("Input", new InputReader());

    CsvParser csvParser = dag.addOperator("CSVParser", new CsvParser());
    AggregationModule aggregator = dag.addModule("Aggregator", new AggregationModule());

    Output out = dag.addOperator("Output", new Output());

    dag.addStream("Input", reader.output, csvParser.in);
    dag.addStream("POJO", csvParser.out, aggregator.inputPOJO);
    dag.addStream("Console", aggregator.finalizedData, out.input);
  }
}
