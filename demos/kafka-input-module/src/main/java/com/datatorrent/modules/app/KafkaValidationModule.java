/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.modules.app;

import java.util.Properties;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.module.KafkaInputModule;

public class KafkaValidationModule implements Module
{
  public static final String DEFAULTOFFSET = "latest";
  @NotNull
  private String topic;
  @NotNull
  private String zookeeper;
  @NotNull
  private String broker;
  private String offsetFilePath;
  @Pattern(
    flags = {Pattern.Flag.CASE_INSENSITIVE},
    regexp = "earliest|latest"
  )
  private String initialOffset = DEFAULTOFFSET;
  private int parallelReads = 0;
  @Min(1L)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;
  private long bandwidth = 0;
  private boolean disableValidation = false;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Logical DAG
    //                      |------ KafkaOutputOperator
    //   RandomGenerator ---|
    //                       ------- Validator
    //   KafkaInputOperator ---------|

    RandomGenerator generator = dag.addOperator("DataGenerator", new RandomGenerator());
    KafkaOutputOperator kafkaOutput = dag.addOperator("KafkaWriter", new KafkaOutputOperator());

    generator.setTuplesBlast(maxTuplesPerWindow);
    generator.setInitialOffset(initialOffset);
    generator.setDisableGenerate(disableValidation);
    kafkaOutput.setTopic(topic);
    Properties configProperties = new Properties();
    configProperties.setProperty("metadata.broker.list",broker);
    kafkaOutput.setConfigProperties(configProperties);

    KafkaInputModule input = dag.addModule("KafkaReader", new KafkaInputModule());
    ValidatorOperator validator = dag.addOperator("Validation", new ValidatorOperator());

    validator.setMaxTuplesPerWindow(maxTuplesPerWindow);
    validator.setDisableValidation(disableValidation);
    input.setTopic(topic);
    input.setZookeeper(zookeeper);
    if(offsetFilePath != null) {
      input.setOffsetFilePath(offsetFilePath);
    }
    input.setInitialOffset(initialOffset);
    if(parallelReads != 0) {
      input.setParallelReads(parallelReads);
    }
    input.setMaxTuplesPerWindow(maxTuplesPerWindow);
    if(bandwidth != 0) {
      input.setBandwidth(bandwidth);
    }

    dag.addStream("ToKafka", generator.output, kafkaOutput.inputPort);
    dag.addStream("ToValidator", generator.integer_data, validator.randomPort);
    dag.addStream("FromKafka", input.output, validator.kafkaPort);
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public String getZookeeper()
  {
    return zookeeper;
  }

  public void setZookeeper(String zookeeper)
  {
    this.zookeeper = zookeeper;
  }

  public String getOffsetFilePath()
  {
    return offsetFilePath;
  }

  public void setOffsetFilePath(String offsetFilePath)
  {
    this.offsetFilePath = offsetFilePath;
  }

  public String getInitialOffset()
  {
    return initialOffset;
  }

  public void setInitialOffset(String initialOffset)
  {
    this.initialOffset = initialOffset;
  }

  public int getParallelReads()
  {
    return parallelReads;
  }

  public void setParallelReads(int parallelReads)
  {
    this.parallelReads = parallelReads;
  }

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public long getBandwidth()
  {
    return bandwidth;
  }

  public void setBandwidth(long bandwidth)
  {
    this.bandwidth = bandwidth;
  }

  public String getBroker()
  {
    return broker;
  }

  public void setBroker(String broker)
  {
    this.broker = broker;
  }

  public boolean isDisableValidation()
  {
    return disableValidation;
  }

  public void setDisableValidation(boolean disableValidation)
  {
    this.disableValidation = disableValidation;
  }
}
