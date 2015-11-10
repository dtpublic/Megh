/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedToPojo;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.modules.delimitedToPojo.DelimitedToPojoConverterModule;

@ApplicationAnnotation(name = "DelimitedToPojoApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputDataGeneratorOperator modifier = dag.addOperator("datagenerator", new InputDataGeneratorOperator());
    DelimitedToPojoConverterModule converter = dag.addModule("delimitedToPojoConverter",
        new DelimitedToPojoConverterModule());
    VerifierOperator verifier = dag.addOperator("verifier", new VerifierOperator());
    dag.addStream("modifiedLine", modifier.output, converter.input).setLocality(Locality.THREAD_LOCAL);
    ;
    dag.addStream("valid", converter.output, verifier.valid);
    dag.addStream("error", converter.error, verifier.error);
  }

}
