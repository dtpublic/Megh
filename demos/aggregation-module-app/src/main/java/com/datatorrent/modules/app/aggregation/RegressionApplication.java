/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.app.aggregation;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.modules.aggregation.AggregationModule;
import com.datatorrent.modules.app.aggregation.regression.InputGenerator;

@ApplicationAnnotation(name = "AggregationModuleRegressionApp")
public class RegressionApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputGenerator generator = dag.addOperator("Input", new InputGenerator());

    AggregationModule aggregator = dag.addModule("Aggregator", new AggregationModule());

    InputGenerator.Validator validator;
    if (conf.getBoolean("dt.application.AggregationModuleRegressionApp.aggregator.verify", true)) {
      validator = dag.addOperator("Validator", new InputGenerator.Validator());
      dag.addStream("Input", generator.out, aggregator.inputPOJO, validator.moduleInput);
      dag.addStream("FinalizedData", aggregator.finalizedData, validator.moduleOutput);
    } else {
      dag.addStream("Input", generator.out, aggregator.inputPOJO);
    }
  }
}
