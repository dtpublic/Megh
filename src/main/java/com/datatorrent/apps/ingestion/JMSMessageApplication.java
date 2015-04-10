/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.apps.ingestion;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.kafka.FileOutputOperator;
import com.datatorrent.lib.io.jms.JMSStringInputOperator;

/**
 * This class defines the application used for ingesting JMS data
 */
@ApplicationAnnotation(name = "JMSMessageIngestionApp")
public class JMSMessageApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Reads from JMS
    JMSStringInputOperator inputOpr = dag.addOperator("MessageReader", new JMSStringInputOperator());
    //Writes to file
    FileOutputOperator outputOpr = dag.addOperator("FileWriter", new FileOutputOperator());
    //Stream connecting reader and writer 
    dag.addStream("JMSData", inputOpr.output, outputOpr.input);
  }

}
