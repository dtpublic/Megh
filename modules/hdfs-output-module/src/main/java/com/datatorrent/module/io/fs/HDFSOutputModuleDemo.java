package com.datatorrent.module.io.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator;

@ApplicationAnnotation(name = "HDFSOutputModuleDemo")
public class HDFSOutputModuleDemo implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileLineInputOperator fileLineInputOperator = dag.addOperator("FileLineInputOperator", new FileLineInputOperator());
    HDFSOutputModule<String> outputModule = dag.addModule("HDFSOutput", new HDFSOutputModule<String>());
    dag.addStream("FileInput To HDFS Output", fileLineInputOperator.output, outputModule.input);
  }
  
  private static Logger LOG = LoggerFactory.getLogger(HDFSOutputModuleDemo.class);
}
