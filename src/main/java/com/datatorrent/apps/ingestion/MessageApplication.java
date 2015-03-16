package com.datatorrent.apps.ingestion;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.kafka.FileOutputOperator;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;

@ApplicationAnnotation(name = "MessageIngestionApp")
public class MessageApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Properties props = new Properties();
    props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "main_group");
    HighlevelKafkaConsumer consumer = new HighlevelKafkaConsumer(props);

    KafkaSinglePortStringInputOperator inputOpr = dag.addOperator("MessageReader", new KafkaSinglePortStringInputOperator());
    inputOpr.setConsumer(consumer);

    FileOutputOperator outputOpr = dag.addOperator("fileWriter", new FileOutputOperator());
    dag.addStream("kafkaData", inputOpr.outputPort, outputOpr.input);
  }

}
