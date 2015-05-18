package com.datatorrent.apps.ingestion;

import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.kafka.FileOutputOperator;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.fs.FilterStreamCodec;
import com.datatorrent.lib.io.fs.FilterStreamProvider;

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
    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    if ("true".equals(conf.get("dt.application.Ingestion.encrypt"))) {
      chainStreamProvider.addStreamProvider(new FilterStreamCodec.CipherSimpleStreamProvider());
    }
    if ("true".equals(conf.get("dt.application.Ingestion.compress"))) {
      chainStreamProvider.addStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());
    }
    if (chainStreamProvider.getStreamProviders().size() > 0) {
      outputOpr.setFilterStreamProvider(chainStreamProvider);
    }

    dag.addStream("kafkaData", inputOpr.outputPort, outputOpr.input);
  }

}
