/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.contrib.kafka.KafkaByteArrayInputOperator;

/**
 * Kafka Input Module will consume data from multiple Partitions of Kafka topic and emits bytes array.
 * This module consists of Malhar kafka input operator with preconfigured parameters like disabling the
 * dynamic partition, default strategy is ONE_TO_MANY and also option to read from a specific partition offset.&nbsp;
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No proxy input port<br>
 * <b>Output</b>: Have only one proxy output port<br>
 * <br>
 * Properties:<br>
 * <b>topic</b>: Name of the topic from where to consume messages <br>
 * <b>zookeeper</b>: Zookeeper quorum of kafka clusters <br>
 * <b>offsetFilePath</b>: Specifies the path of the file which contains the offset information for all the partitions.
 *                        Kafka input module will start consuming messages from these offsets. This is an optional
 *                        parameter. <br>
 * <b>initialOffset</b>: Indicates the type of offset i.e, “earliest or latest”. If initialOffset is “latest”, then the
 *                       operator consumes messages from latest point of Kafka queue. If initialOffset is “earliest”,
 *                       then the operator consumes messages starting from message queue.Default value is “latest”.<br>
 * <b>parallelReads</b>: Indicatest the number of operator instances <br>
 * <b>maxTuplesPerWindow</b>: Specifies the number of maximum messages emitting in each application window. <br>
 * <b>strategy</b>: Partition strategy be ONE_TO_ONE or ONE_TO_MANY. By default, the value is ONE_TO_MANY <br>
 * <b>bandwidth</b>: Specifies the maximum possible bandwidth in MB and will be used to consume messages. <br>
 * <br>
 * Compile time checks:<br>
 * None <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName Kafka Input Module
 * @category Messaging
 * @tags input operator
 *
 */
public class KafkaInputModule implements Module
{
  public transient ProxyOutputPort<byte[]> output = new ProxyOutputPort<byte[]>();

  @NotNull
  private String topic;

  @NotNull
  private String zookeeper;

  private String offsetFilePath;

  @Pattern(flags = {Pattern.Flag.CASE_INSENSITIVE}, regexp = "earliest|latest")
  private String initialOffset = "latest";

  @Min(1)
  private int parallelReads = 1;

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;

  private String strategy = "one_to_many";

  private long bandwidth;

  /**
   * Kafka Input module dag consists of single Kafka input operator
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaByteArrayInputOperator input = dag.addOperator("MessageReader", new KafkaByteArrayInputOperator());

    // Set the offset manager if the offsetFilePath is not null
    if (offsetFilePath != null) {
      HdfsOffsetManager offsetManager = new HdfsOffsetManager();
      offsetManager.setDelimiter(",");
      offsetManager.setOffsetFilePath(offsetFilePath);
      offsetManager.setTopicName(topic);
      input.setOffsetManager(offsetManager);
    }

    // Disabling dynamic partition
    input.setRepartitionInterval(-1);

    // Sets the module parameters to operator
    input.setTopic(topic);
    input.setZookeeper(zookeeper);

    input.setInitialOffset(initialOffset);

    input.setMaxTuplesPerWindow(maxTuplesPerWindow);

    input.setInitialPartitionCount(parallelReads);

    input.setStrategy(strategy);

    if (bandwidth != 0) {
      input.getBandwidthManager().setBandwidth(bandwidth);
    }

    // Setting the proxy port
    output.set(input.outputPort);
  }

  /**
   * Returns the kafka topic from where to consume messages
   * @return the kafka topic
   */
  public String getTopic()
  {
    return topic;
  }

  /**
   * Set the kafka topic from where to consume messages
   * @param topic Kafka topic sets to this module
   */
  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  /**
   * Returns the zookeeper quorum of the Kafka cluster(s)
   * @return the zookeeper quorum string
   */
  public String getZookeeper()
  {
    return zookeeper;
  }

  /**
   * Set the zookeeper quorum of the Kafka cluster(s) you want to consume data from.
   * @param zookeeper zookeeper quorum string to set this module
   */
  public void setZookeeper(String zookeeper)
  {
    this.zookeeper = zookeeper;
  }

  /**
   * Returns the path of the file which contains the offsets info.
   * @return the path of the file
   */
  public String getOffsetFilePath()
  {
    return offsetFilePath;
  }

  /**
   * File path consists of offsets info. Module start consuming messages from these offsets.
   * This is an optional parameter.
   * @param offsetFilePath
   */
  public void setOffsetFilePath(String offsetFilePath)
  {
    this.offsetFilePath = offsetFilePath;
  }

  /**
   * Return the initial offset.
   * @return the initial offset.
   */
  public String getInitialOffset()
  {
    return initialOffset;
  }

  /**
   * Sets the initialOffset. By default, the value is latest. If initialOffset is “latest” then the module
   * consumes messages from latest point of Kafka queue. If initialOffset is “earliest”, then the module
   * consumes messages starting from message queue. This can be overridden by offsetFilePath.
   * @param initialOffset initialOffset
   */
  public void setInitialOffset(String initialOffset)
  {
    this.initialOffset = initialOffset;
  }

  /**
   * Returns the number of parallel reads.
   * @return the number of parallel reads.
   */
  public int getParallelReads()
  {
    return parallelReads;
  }

  /**
   * Sets the number of parallel reads. This option is enabled in case of ONE_TO_MANY strategy.
   * By default, the value is 1.
   * @param parallelReads parallelReads sets to this module
   */
  public void setParallelReads(int parallelReads)
  {
    this.parallelReads = parallelReads;
  }

  /**
   * Returns the maximum number of messages emitted in each streaming window
   * @return the maximum number of messages per window
   */
  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  /**
   * Controls the maximum number of messages emitted in each streaming window from this module.
   * Minimum value is 1. Default value = MAX_VALUE
   * @param maxTuplesPerWindow maxTuplesPerWindow to set
   */
  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  /**
   * Returns the partition strategy sets to this module
   * @return the partition strategy
   */
  public String getStrategy()
  {
    return strategy;
  }

  /**
   * Module supports two types of partitioning strategies, ONE_TO_ONE and ONE_TO_MANY.
   * By default, the value is ONE_TO_MANY
   * @param strategy strategy to set
   */
  public void setStrategy(String strategy)
  {
    this.strategy = strategy;
  }

  /**
   * Returns the bandwidth limit in MB
   * @return the bandwidth limit.
   */
  public long getBandwidth()
  {
    return bandwidth;
  }

  /**
   * Sets the bandwidth limit on network usage in MB
   * @param bandwidth bandwidth to set
   */
  public void setBandwidth(long bandwidth)
  {
    this.bandwidth = bandwidth;
  }
}
