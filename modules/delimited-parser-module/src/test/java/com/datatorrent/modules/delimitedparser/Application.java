/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.modules.delimitedparser;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "ParserApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputDataGeneratorOperator datagenerator = dag.addOperator("datagenerator", new InputDataGeneratorOperator());
    DelimitedParserModule parser = dag.addModule("delimitedParser", new DelimitedParserModule());
    VerifierOperator verifier = dag.addOperator("verifier", new VerifierOperator());
    dag.addStream("input", datagenerator.output, parser.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("parsedObject", parser.parsedData, verifier.parsedObject);
    dag.addStream("pojo", parser.pojo, verifier.pojo);
    dag.addStream("error", parser.error, verifier.error);
  }

}
